# ─── Version ──────────────────────────────────────────────────────────────────
# Inferred from git tags at build time; override with: make docker VERSION=1.2.3
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || \
             grep -oP 'const version = "\K[^"]+' cmd/jaiscloud/main.go 2>/dev/null || \
             echo "dev")

# Optional registry prefix, e.g. REGISTRY=ghcr.io/myorg
# When set, images are tagged as both <image>:<version> and <registry>/<image>:<version>.
REGISTRY ?=

# ─── E2E test knobs ───────────────────────────────────────────────────────────
# Spark image for EMR (Docker) tests.  Override: make test-e2e-docker SPARK_E2E_DOCKER_IMAGE=my/spark:3.5
SPARK_E2E_DOCKER_IMAGE  ?= apache/spark:3.5.0
# Iceberg-enabled Spark image for iceberg tests.  Override: make test-e2e-iceberg SPARK_E2E_ICEBERG_IMAGE=my/iceberg:latest
SPARK_E2E_ICEBERG_IMAGE ?= spark-iceberg-test
# Spark image for EMR Containers (K8s) tests (must be loaded in docker-desktop).
SPARK_E2E_SPARK_IMAGE   ?= apache/spark:3.5.0
# K8s namespace used by EMR Containers tests against docker-desktop.
SPARK_E2E_K8S_NAMESPACE ?= default

# ─── Server knobs (used when e2e targets start jaiscloud) ─────────────────────
JAISCLOUD_DSN          ?= postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud
JAISCLOUD_PORT         ?= 4566
JAISCLOUD_HOST         ?= http://localhost:$(JAISCLOUD_PORT)
JAISCLOUD_SPARK_MODE   ?= off
JAISCLOUD_SPARK_IMAGE  ?=
# K8s API server resolved from docker-desktop kubeconfig at make-time.
# Override: make test-e2e-k8s JAISCLOUD_K8S_APISERVER=https://my-cluster:6443
JAISCLOUD_K8S_APISERVER ?= $(shell kubectl config view --context docker-desktop --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null)
JAISCLOUD_K8S_NAMESPACE         ?=
JAISCLOUD_K8S_CA_FILE           ?=
JAISCLOUD_K8S_CLIENT_CERT_FILE  ?=
JAISCLOUD_K8S_CLIENT_KEY_FILE   ?=

IMAGE := jaiscloud

.PHONY: help build test \
        docker clean \
        test-e2e-docker test-e2e-k8s test-e2e-eventbridge test-e2e-spark test-e2e-iceberg test-e2e \
        stop-server \
        _check-docker-prereq _check-k8s-prereq _check-iceberg-prereq \
        _build-for-e2e _restart-server _setup-k8s-creds

## help: Print this help.
help:
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':' | sed 's/^/  /'

# ─── Binary ───────────────────────────────────────────────────────────────────

## build: Compile the jaiscloud binary (CGO_ENABLED=0, static).
build:
	go build -trimpath -ldflags="-s -w" -o jaiscloud ./cmd/jaiscloud/

# ─── Tests ────────────────────────────────────────────────────────────────────

## test: Run host-module unit tests with the race detector.
test:
	go test -race ./internal/...

# ─── Docker ───────────────────────────────────────────────────────────────────

## docker: Build jaiscloud:<VERSION> — static scratch-based image, supports lite and full mode.
docker:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--tag $(IMAGE):$(VERSION) \
		--tag $(IMAGE):latest \
		--file Dockerfile \
		.
ifdef REGISTRY
	docker tag $(IMAGE):$(VERSION) $(REGISTRY)/$(IMAGE):$(VERSION)
	docker tag $(IMAGE):latest     $(REGISTRY)/$(IMAGE):latest
endif

# ─── Misc ─────────────────────────────────────────────────────────────────────

## clean: Remove the built binary.
clean:
	rm -f jaiscloud

# ─── E2E Tests ────────────────────────────────────────────────────────────────

# Internal: build binary from current source.
_build-for-e2e:
	go build -o jaiscloud ./cmd/jaiscloud/

# Internal: stop any running instance, start a fresh one, wait for health.
# The server keeps running after tests so logs stay accessible; call stop-server to clean up.
_restart-server: _build-for-e2e
	@pkill -f "jaiscloud start" 2>/dev/null || true
	@sleep 1
	@JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  JAISCLOUD_SPARK_MODE=$(JAISCLOUD_SPARK_MODE) \
	  $(if $(JAISCLOUD_SPARK_IMAGE),JAISCLOUD_SPARK_IMAGE=$(JAISCLOUD_SPARK_IMAGE)) \
	  $(if $(JAISCLOUD_K8S_APISERVER),JAISCLOUD_K8S_APISERVER=$(JAISCLOUD_K8S_APISERVER)) \
	  $(if $(JAISCLOUD_K8S_NAMESPACE),JAISCLOUD_K8S_NAMESPACE=$(JAISCLOUD_K8S_NAMESPACE)) \
	  $(if $(JAISCLOUD_K8S_CA_FILE),JAISCLOUD_K8S_CA_FILE=$(JAISCLOUD_K8S_CA_FILE)) \
	  $(if $(JAISCLOUD_K8S_CLIENT_CERT_FILE),JAISCLOUD_K8S_CLIENT_CERT_FILE=$(JAISCLOUD_K8S_CLIENT_CERT_FILE)) \
	  $(if $(JAISCLOUD_K8S_CLIENT_KEY_FILE),JAISCLOUD_K8S_CLIENT_KEY_FILE=$(JAISCLOUD_K8S_CLIENT_KEY_FILE)) \
	  ./jaiscloud start --mode full --dsn "$(JAISCLOUD_DSN)" \
	  > /tmp/jaiscloud-e2e.log 2>&1 &
	@echo "Waiting for jaiscloud to be ready on $(JAISCLOUD_HOST)..."
	@n=0; until curl -sf $(JAISCLOUD_HOST)/_jaiscloud/health > /dev/null 2>&1; do \
	  n=$$((n+1)); \
	  if [ $$n -ge 30 ]; then \
	    echo "ERROR: jaiscloud did not become healthy within 30s — check /tmp/jaiscloud-e2e.log"; \
	    exit 1; \
	  fi; \
	  sleep 1; \
	done; \
	echo "jaiscloud ready (log: /tmp/jaiscloud-e2e.log)"

# Internal: extract docker-desktop K8s credentials from kubeconfig to temp files.
# Writes CA cert, client cert, and client key so the server can authenticate.
_setup-k8s-creds:
	@kubectl config view --context docker-desktop --minify --raw \
	  -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' \
	  | base64 -d > /tmp/jaiscloud-k8s-ca.crt
	@kubectl config view --context docker-desktop --minify --raw \
	  -o jsonpath='{.users[0].user.client-certificate-data}' \
	  | base64 -d > /tmp/jaiscloud-k8s-client.crt
	@kubectl config view --context docker-desktop --minify --raw \
	  -o jsonpath='{.users[0].user.client-key-data}' \
	  | base64 -d > /tmp/jaiscloud-k8s-client.key
	@chmod 600 /tmp/jaiscloud-k8s-client.key
	@echo "K8s credentials written to /tmp/jaiscloud-k8s-{ca,client}.{crt,key}"

## stop-server: Stop the background jaiscloud instance started by e2e targets.
stop-server:
	@pkill -f "jaiscloud start" 2>/dev/null && echo "jaiscloud stopped" || echo "jaiscloud was not running"

# Internal prereq: Docker daemon running and SPARK_E2E_DOCKER_IMAGE set.
_check-docker-prereq:
	@if [ -z "$(SPARK_E2E_DOCKER_IMAGE)" ]; then \
	  echo "ERROR: SPARK_E2E_DOCKER_IMAGE is not set"; exit 1; fi
	@docker info > /dev/null 2>&1 || \
	  (echo "ERROR: Docker daemon is not running"; exit 1)

# Internal prereq: docker-desktop K8s reachable and SPARK_E2E_SPARK_IMAGE set.
_check-k8s-prereq:
	@if [ -z "$(SPARK_E2E_SPARK_IMAGE)" ]; then \
	  echo "ERROR: SPARK_E2E_SPARK_IMAGE is not set (must be an image loaded in docker-desktop)"; exit 1; fi
	@kubectl --context docker-desktop cluster-info > /dev/null 2>&1 || \
	  (echo "ERROR: docker-desktop Kubernetes cluster is not reachable (is Kubernetes enabled in Docker Desktop?)"; exit 1)

# Internal prereq: iceberg image exists locally.
_check-iceberg-prereq:
	@if [ -z "$(SPARK_E2E_ICEBERG_IMAGE)" ]; then \
	  echo "ERROR: SPARK_E2E_ICEBERG_IMAGE is not set"; exit 1; fi
	@docker image inspect $(SPARK_E2E_ICEBERG_IMAGE) > /dev/null 2>&1 || \
	  (echo "ERROR: image '$(SPARK_E2E_ICEBERG_IMAGE)' not found locally — build or pull it first"; exit 1)

## test-e2e-docker: Run EMR (Docker) Spark e2e tests.
##                  Tests: TestSparkJob_Docker_* (4 tests)
##                  Requires: Docker daemon, SPARK_E2E_DOCKER_IMAGE (default: apache/spark:3.5.0)
test-e2e-docker: _check-docker-prereq _build-for-e2e
	$(MAKE) _restart-server JAISCLOUD_SPARK_MODE=docker JAISCLOUD_SPARK_IMAGE=$(SPARK_E2E_DOCKER_IMAGE)
	SPARK_E2E_DOCKER_IMAGE=$(SPARK_E2E_DOCKER_IMAGE) \
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m \
	  -run TestSparkJob_Docker \
	  ./tests/full_mode/plugin/

## test-e2e-k8s: Run EMR Containers (K8s) Spark e2e tests against docker-desktop.
##               Tests: TestSparkJob_K8s_* (4 tests)
##               Requires: docker-desktop Kubernetes enabled, SPARK_E2E_SPARK_IMAGE set explicitly.
##               Example: make test-e2e-k8s SPARK_E2E_SPARK_IMAGE=apache/spark:3.5.0
test-e2e-k8s: _check-k8s-prereq _build-for-e2e _setup-k8s-creds
	$(MAKE) _restart-server JAISCLOUD_SPARK_MODE=k8s \
	  JAISCLOUD_K8S_APISERVER=$(JAISCLOUD_K8S_APISERVER) \
	  JAISCLOUD_K8S_NAMESPACE=$(SPARK_E2E_K8S_NAMESPACE) \
	  JAISCLOUD_K8S_CA_FILE=/tmp/jaiscloud-k8s-ca.crt \
	  JAISCLOUD_K8S_CLIENT_CERT_FILE=/tmp/jaiscloud-k8s-client.crt \
	  JAISCLOUD_K8S_CLIENT_KEY_FILE=/tmp/jaiscloud-k8s-client.key
	SPARK_E2E_SPARK_IMAGE=$(SPARK_E2E_SPARK_IMAGE) \
	SPARK_E2E_K8S_NAMESPACE=$(SPARK_E2E_K8S_NAMESPACE) \
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 15m \
	  -run TestSparkJob_K8s \
	  ./tests/full_mode/plugin/

## test-e2e-eventbridge: Run EventBridge notification e2e tests (mock executor — no Docker/K8s needed).
##                       Tests: TestSparkJob_EventBridge_* (5 tests)
test-e2e-eventbridge: _build-for-e2e
	$(MAKE) _restart-server JAISCLOUD_SPARK_MODE=mock
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m \
	  -run TestSparkJob_EventBridge \
	  ./tests/full_mode/plugin/

## test-e2e-spark: Run all Spark plugin e2e tests (Docker + EventBridge; K8s if SPARK_E2E_SPARK_IMAGE set).
##                 Tests: all 13 TestSparkJob_* tests
##                 Requires: Docker daemon, SPARK_E2E_DOCKER_IMAGE (default: apache/spark:3.5.0)
##                 Phase 1 (mock): Docker + EventBridge tests
##                 Phase 2 (k8s, only if SPARK_E2E_SPARK_IMAGE is set): K8s tests
test-e2e-spark: _check-docker-prereq _build-for-e2e
	$(MAKE) _restart-server JAISCLOUD_SPARK_MODE=docker JAISCLOUD_SPARK_IMAGE=$(SPARK_E2E_DOCKER_IMAGE)
	SPARK_E2E_DOCKER_IMAGE=$(SPARK_E2E_DOCKER_IMAGE) \
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 15m \
	  -run "TestSparkJob_Docker|TestSparkJob_EventBridge" \
	  ./tests/full_mode/plugin/
	@if [ -n "$(SPARK_E2E_SPARK_IMAGE)" ]; then \
	  echo "--- K8s phase (SPARK_E2E_SPARK_IMAGE=$(SPARK_E2E_SPARK_IMAGE)) ---"; \
	  $(MAKE) _setup-k8s-creds; \
	  $(MAKE) _restart-server JAISCLOUD_SPARK_MODE=k8s \
	    JAISCLOUD_K8S_APISERVER=$(JAISCLOUD_K8S_APISERVER) \
	    JAISCLOUD_K8S_NAMESPACE=$(SPARK_E2E_K8S_NAMESPACE) \
	    JAISCLOUD_K8S_CA_FILE=/tmp/jaiscloud-k8s-ca.crt \
	    JAISCLOUD_K8S_CLIENT_CERT_FILE=/tmp/jaiscloud-k8s-client.crt \
	    JAISCLOUD_K8S_CLIENT_KEY_FILE=/tmp/jaiscloud-k8s-client.key; \
	  SPARK_E2E_SPARK_IMAGE=$(SPARK_E2E_SPARK_IMAGE) \
	  SPARK_E2E_K8S_NAMESPACE=$(SPARK_E2E_K8S_NAMESPACE) \
	  JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	    go test -v -tags spark_e2e -timeout 15m \
	    -run TestSparkJob_K8s \
	    ./tests/full_mode/plugin/; \
	else \
	  echo "Skipping K8s tests (SPARK_E2E_SPARK_IMAGE not set)"; \
	fi

## test-e2e-iceberg: Run all Iceberg e2e tests.
##                   Tests: TestIceberg_GlueCatalog_* (6 tests)
##                   Requires: SPARK_E2E_ICEBERG_IMAGE present locally (default: spark-iceberg-test)
test-e2e-iceberg: _check-iceberg-prereq _build-for-e2e
	$(MAKE) _restart-server JAISCLOUD_SPARK_MODE=off
	SPARK_E2E_ICEBERG_IMAGE=$(SPARK_E2E_ICEBERG_IMAGE) \
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags iceberg_e2e -timeout 30m \
	  ./tests/full_mode/iceberg/

## test-e2e: Run all e2e tests: Spark plugin suite then Iceberg suite.
test-e2e: test-e2e-spark test-e2e-iceberg
