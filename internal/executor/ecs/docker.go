package ecs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

const ecsDockerSocket = "/var/run/docker.sock"

type dockerExecutor struct {
	client  *http.Client
	logsAPI LogsIngestor
}

func newDockerExecutor(logsAPI LogsIngestor) *dockerExecutor {
	return &dockerExecutor{
		logsAPI: logsAPI,
		client: &http.Client{
			Timeout: 5 * time.Minute,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", ecsDockerSocket)
				},
			},
		},
	}
}

func (e *dockerExecutor) Run(ctx context.Context, spec TaskSpec) (TaskHandle, error) {
	var ids []string
	var firstID string

	for i, cs := range spec.Containers {
		env := buildEnvList(cs.Env)
		hostCfg := map[string]any{
			"AutoRemove": false,
		}
		// Subsequent containers share the network namespace of the first.
		if i > 0 && firstID != "" {
			hostCfg["NetworkMode"] = "container:" + firstID
		}
		if cs.Memory > 0 {
			hostCfg["Memory"] = cs.Memory
		}

		// Port bindings (first container only; subsequent share network).
		if i == 0 && len(cs.PortMappings) > 0 {
			portBindings := map[string]any{}
			exposedPorts := map[string]any{}
			for _, pm := range cs.PortMappings {
				proto := pm.Protocol
				if proto == "" {
					proto = "tcp"
				}
				key := fmt.Sprintf("%d/%s", pm.ContainerPort, proto)
				hostPort := ""
				if pm.HostPort > 0 {
					hostPort = fmt.Sprintf("%d", pm.HostPort)
				}
				portBindings[key] = []map[string]any{{"HostPort": hostPort}}
				exposedPorts[key] = map[string]any{}
			}
			hostCfg["PortBindings"] = portBindings
		}

		labels := map[string]string{
			"jaiscloud.io/service": "ecs",
			"jaiscloud.io/task":    spec.TaskARN,
		}

		body, _ := json.Marshal(map[string]any{
			"Image":   cs.Image,
			"Env":     env,
			"Cmd":     cs.Cmd,
			"Labels":  labels,
			"HostConfig": hostCfg,
		})

		name := ecsContainerName(spec.TaskARN, cs.Name)
		createURL := fmt.Sprintf("http://localhost/v1.41/containers/create?name=%s", name)
		respBody, status, err := e.dockerCall(ctx, http.MethodPost, createURL, body)
		if err != nil {
			return TaskHandle{}, fmt.Errorf("ecs docker: create %s: %w", cs.Name, err)
		}
		if status >= 300 {
			return TaskHandle{}, fmt.Errorf("ecs docker: create %s HTTP %d: %s", cs.Name, status, respBody)
		}

		var createResp struct{ Id string }
		json.Unmarshal(respBody, &createResp)

		startURL := fmt.Sprintf("http://localhost/v1.41/containers/%s/start", createResp.Id)
		_, status, err = e.dockerCall(ctx, http.MethodPost, startURL, nil)
		if err != nil {
			return TaskHandle{}, fmt.Errorf("ecs docker: start %s: %w", cs.Name, err)
		}
		if status >= 300 {
			return TaskHandle{}, fmt.Errorf("ecs docker: start %s HTTP %d", cs.Name, status)
		}

		if i == 0 {
			firstID = createResp.Id
		}
		ids = append(ids, createResp.Id)
		slog.Info("ecs docker: started container", "name", cs.Name, "id", createResp.Id[:12], "task", spec.TaskARN)

		// Stream logs if awslogs configured.
		if e.logsAPI != nil {
			logCfg := cs.LogConfig
			if logCfg.LogDriver == "" {
				logCfg = spec.LogConfig
			}
			if logCfg.LogDriver == "awslogs" {
				taskID := ecsShortID(spec.TaskARN)
				go e.streamContainerLogs(context.Background(), createResp.Id, logCfg, cs.Name, taskID)
			}
		}
	}

	return TaskHandle{ContainerIDs: ids, Mode: ModeDocker}, nil
}

func (e *dockerExecutor) Wait(ctx context.Context, handle TaskHandle) error {
	for _, id := range handle.ContainerIDs {
		for {
			st, err := e.inspectContainer(ctx, id)
			if err != nil {
				return err
			}
			if st == "exited" || st == "dead" {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
	return nil
}

func (e *dockerExecutor) Stop(ctx context.Context, handle TaskHandle) error {
	for _, id := range handle.ContainerIDs {
		stopURL := fmt.Sprintf("http://localhost/v1.41/containers/%s/stop", id)
		e.dockerCall(ctx, http.MethodPost, stopURL, nil) //nolint:errcheck
	}
	return nil
}

func (e *dockerExecutor) StatusOf(ctx context.Context, handle TaskHandle) (Status, error) {
	if len(handle.ContainerIDs) == 0 {
		return Status{LastStatus: "STOPPED"}, nil
	}

	var containers []ContainerStatus
	allStopped := true
	anyRunning := false

	for _, id := range handle.ContainerIDs {
		raw, status, err := e.dockerCall(ctx, http.MethodGet, fmt.Sprintf("http://localhost/v1.41/containers/%s/json", id), nil)
		if err != nil || status >= 300 {
			containers = append(containers, ContainerStatus{Name: id, LastStatus: "STOPPED"})
			continue
		}
		var info struct {
			Name  string `json:"Name"`
			State struct {
				Status   string `json:"Status"`
				ExitCode int    `json:"ExitCode"`
			} `json:"State"`
		}
		json.Unmarshal(raw, &info)
		cs := ContainerStatus{
			Name:       strings.TrimPrefix(info.Name, "/"),
			LastStatus: dockerStateToECS(info.State.Status),
		}
		if info.State.Status == "exited" || info.State.Status == "dead" {
			code := info.State.ExitCode
			cs.ExitCode = &code
		} else {
			allStopped = false
		}
		if info.State.Status == "running" {
			anyRunning = true
		}
		containers = append(containers, cs)
	}

	lastStatus := "PROVISIONING"
	if anyRunning {
		lastStatus = "RUNNING"
	} else if allStopped {
		lastStatus = "STOPPED"
	}

	return Status{LastStatus: lastStatus, Containers: containers}, nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (e *dockerExecutor) inspectContainer(ctx context.Context, id string) (string, error) {
	body, status, err := e.dockerCall(ctx, http.MethodGet,
		fmt.Sprintf("http://localhost/v1.41/containers/%s/json", id), nil)
	if err != nil {
		return "", err
	}
	if status >= 300 {
		return "dead", nil
	}
	var info struct {
		State struct{ Status string } `json:"State"`
	}
	json.Unmarshal(body, &info)
	return info.State.Status, nil
}

func (e *dockerExecutor) streamContainerLogs(ctx context.Context, id string, cfg LogConfig, containerName, taskID string) {
	logsURL := fmt.Sprintf("http://localhost/v1.41/containers/%s/logs?follow=true&stdout=1&stderr=1", id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, logsURL, nil)
	if err != nil {
		return
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	StreamLogs(ctx, e.logsAPI, cfg, containerName, taskID, resp.Body)
}

func (e *dockerExecutor) dockerCall(ctx context.Context, method, url string, body []byte) ([]byte, int, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return rb, resp.StatusCode, nil
}

func buildEnvList(env map[string]string) []string {
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}

func ecsContainerName(taskARN, containerName string) string {
	short := ecsShortID(taskARN)
	sanitized := strings.NewReplacer(":", "-", "/", "-").Replace(containerName)
	return "jc-ecs-" + short + "-" + sanitized
}

func ecsShortID(taskARN string) string {
	// arn:aws:ecs:...:task/cluster/id → last segment
	for i := len(taskARN) - 1; i >= 0; i-- {
		if taskARN[i] == '/' {
			s := taskARN[i+1:]
			if len(s) > 12 {
				s = s[:12]
			}
			return s
		}
	}
	if len(taskARN) > 12 {
		return taskARN[:12]
	}
	return taskARN
}

func dockerStateToECS(dockerState string) string {
	switch dockerState {
	case "created":
		return "PROVISIONING"
	case "running":
		return "RUNNING"
	case "paused":
		return "RUNNING"
	case "restarting":
		return "PENDING"
	case "exited", "dead":
		return "STOPPED"
	default:
		return "PENDING"
	}
}
