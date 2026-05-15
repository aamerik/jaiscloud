package ecs

// Mode selects the container backend for ECS task execution.
type Mode int

const (
	ModeMock   Mode = iota
	ModeDocker Mode = iota
	ModeK8s    Mode = iota
)

// TaskSpec describes everything needed to launch an ECS task.
type TaskSpec struct {
	ClusterName       string
	TaskARN           string
	TaskDefName       string
	Containers        []ContainerSpec
	LogConfig         LogConfig
	TaskRoleARN       string
	AccountID         string
	Region            string
	JaisCloudEndpoint string
}

// ContainerSpec describes one container in a task.
type ContainerSpec struct {
	Name         string
	Image        string
	Cmd          []string
	Env          map[string]string
	Memory       int64
	CPU          int64
	PortMappings []PortMapping
	LogConfig    LogConfig
}

// PortMapping maps a container port to a host port.
type PortMapping struct {
	ContainerPort int
	HostPort      int
	Protocol      string // "tcp" | "udp"
}

// LogConfig holds the log driver and its options for a container or task.
type LogConfig struct {
	LogDriver string            // "awslogs" | ""
	Options   map[string]string // awslogs-group, awslogs-stream-prefix, awslogs-region, awslogs-create-group
}

// TaskHandle identifies a running task across executor backends.
type TaskHandle struct {
	PodName      string   // k8s
	ContainerIDs []string // docker
	NetworkID    string   // docker network for multi-container
	Mode         Mode
}

// ContainerStatus holds the observed state of one container in a task.
type ContainerStatus struct {
	Name       string
	LastStatus string
	ExitCode   *int
}

// Status is the aggregated state of a task.
type Status struct {
	LastStatus string // PROVISIONING|PENDING|RUNNING|DEPROVISIONING|STOPPED
	Containers []ContainerStatus
}
