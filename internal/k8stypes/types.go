// Package k8stypes contains the minimal Kubernetes API type stubs used by
// JaisCloud executors (Spark, Lambda) and the platform apply helpers.
// Hand-rolled to avoid a client-go dependency; field names and JSON tags
// match the k8s.io/api/core/v1 wire format exactly.
package k8stypes

// ── Pod / container types ──────────────────────────────────────────────────

type PodSpec struct {
	RestartPolicy             string                     `json:"restartPolicy,omitempty"`
	ServiceAccountName        string                     `json:"serviceAccountName,omitempty"`
	InitContainers            []Container                `json:"initContainers,omitempty"`
	Containers                []Container                `json:"containers"`
	Volumes                   []Volume                   `json:"volumes,omitempty"`
	NodeSelector              map[string]string          `json:"nodeSelector,omitempty"`
	Tolerations               []Toleration               `json:"tolerations,omitempty"`
	Affinity                  *Affinity                  `json:"affinity,omitempty"`
	TopologySpreadConstraints []TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	SecurityContext           *PodSecurityContext        `json:"securityContext,omitempty"`
}

type Container struct {
	Name            string           `json:"name"`
	Image           string           `json:"image"`
	ImagePullPolicy string           `json:"imagePullPolicy,omitempty"`
	Command         []string         `json:"command,omitempty"`
	Args            []string         `json:"args,omitempty"`
	Env             []EnvVar         `json:"env,omitempty"`
	VolumeMounts    []VolumeMount    `json:"volumeMounts,omitempty"`
	Resources       *Resources       `json:"resources,omitempty"`
	Ports           []ContainerPort  `json:"ports,omitempty"`
	SecurityContext *SecurityContext `json:"securityContext,omitempty"`
	ReadinessProbe  *Probe           `json:"readinessProbe,omitempty"`
	LivenessProbe   *Probe           `json:"livenessProbe,omitempty"`
}

type EnvVar struct {
	Name      string        `json:"name"`
	Value     string        `json:"value,omitempty"`
	ValueFrom *EnvVarSource `json:"valueFrom,omitempty"`
}

type EnvVarSource struct {
	SecretKeyRef    *SecretKeySelector    `json:"secretKeyRef,omitempty"`
	ConfigMapKeyRef *ConfigMapKeySelector `json:"configMapKeyRef,omitempty"`
	FieldRef        *ObjectFieldSelector  `json:"fieldRef,omitempty"`
}

type SecretKeySelector struct {
	Name     string `json:"name"`
	Key      string `json:"key"`
	Optional *bool  `json:"optional,omitempty"`
}

type ConfigMapKeySelector struct {
	Name     string `json:"name"`
	Key      string `json:"key"`
	Optional *bool  `json:"optional,omitempty"`
}

type VolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	SubPath   string `json:"subPath,omitempty"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type ContainerPort struct {
	ContainerPort int    `json:"containerPort"`
	Protocol      string `json:"protocol,omitempty"`
}

type Resources struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}

type Probe struct {
	TCPSocket           *TCPSocketAction `json:"tcpSocket,omitempty"`
	InitialDelaySeconds int              `json:"initialDelaySeconds,omitempty"`
	PeriodSeconds       int              `json:"periodSeconds,omitempty"`
	FailureThreshold    int              `json:"failureThreshold,omitempty"`
}

type TCPSocketAction struct {
	Port int `json:"port"`
}

// ── Volume types ───────────────────────────────────────────────────────────

type Volume struct {
	Name      string        `json:"name"`
	ConfigMap *ConfigMapVol `json:"configMap,omitempty"`
	Secret    *SecretVol    `json:"secret,omitempty"`
	EmptyDir  *EmptyDirVol  `json:"emptyDir,omitempty"`
	Projected *ProjectedVol `json:"projected,omitempty"`
	PVC       *PVCVol       `json:"persistentVolumeClaim,omitempty"`
	CSI       *CSIVol       `json:"csi,omitempty"`
	HostPath  *HostPathVol  `json:"hostPath,omitempty"`
}

type ConfigMapVol struct {
	Name        string      `json:"name"`
	DefaultMode *int32      `json:"defaultMode,omitempty"`
	Items       []KeyToPath `json:"items,omitempty"`
	Optional    *bool       `json:"optional,omitempty"`
}

type SecretVol struct {
	SecretName  string      `json:"secretName"`
	DefaultMode *int32      `json:"defaultMode,omitempty"`
	Items       []KeyToPath `json:"items,omitempty"`
	Optional    *bool       `json:"optional,omitempty"`
}

type EmptyDirVol struct {
	Medium    string `json:"medium,omitempty"`
	SizeLimit string `json:"sizeLimit,omitempty"`
}

type ProjectedVol struct {
	Sources     []ProjectionElement `json:"sources"`
	DefaultMode *int32              `json:"defaultMode,omitempty"`
}

type ProjectionElement struct {
	ServiceAccountToken *SATokenProjection     `json:"serviceAccountToken,omitempty"`
	Secret              *ConfigMapVol          `json:"secret,omitempty"`
	ConfigMap           *ConfigMapVol          `json:"configMap,omitempty"`
	DownwardAPI         *DownwardAPIProjection `json:"downwardAPI,omitempty"`
}

type SATokenProjection struct {
	Audience          string `json:"audience"`
	ExpirationSeconds int64  `json:"expirationSeconds,omitempty"`
	Path              string `json:"path"`
}

type DownwardAPIProjection struct {
	Items []DownwardAPIItem `json:"items"`
}

type DownwardAPIItem struct {
	Path     string               `json:"path"`
	FieldRef *ObjectFieldSelector `json:"fieldRef,omitempty"`
}

type ObjectFieldSelector struct {
	FieldPath string `json:"fieldPath"`
}

type PVCVol struct {
	ClaimName string `json:"claimName"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type CSIVol struct {
	Driver           string            `json:"driver"`
	ReadOnly         bool              `json:"readOnly,omitempty"`
	FSType           string            `json:"fsType,omitempty"`
	VolumeAttributes map[string]string `json:"volumeAttributes,omitempty"`
}

type HostPathVol struct {
	Path string `json:"path"`
	Type string `json:"type,omitempty"`
}

type KeyToPath struct {
	Key  string `json:"key"`
	Path string `json:"path"`
	Mode *int32 `json:"mode,omitempty"`
}

// ── Scheduling / security types ────────────────────────────────────────────

type Toleration struct {
	Key               string `json:"key,omitempty"`
	Operator          string `json:"operator,omitempty"`
	Value             string `json:"value,omitempty"`
	Effect            string `json:"effect,omitempty"`
	TolerationSeconds *int64 `json:"tolerationSeconds,omitempty"`
}

type Affinity struct {
	NodeAffinity    *NodeAffinity    `json:"nodeAffinity,omitempty"`
	PodAffinity     *PodAffinity     `json:"podAffinity,omitempty"`
	PodAntiAffinity *PodAntiAffinity `json:"podAntiAffinity,omitempty"`
}

type NodeAffinity struct {
	RequiredDuringSchedulingIgnoredDuringExecution *NodeSelector `json:"requiredDuringSchedulingIgnoredDuringExecution,omitempty"`
}

type NodeSelector struct {
	NodeSelectorTerms []NodeSelectorTerm `json:"nodeSelectorTerms"`
}

type NodeSelectorTerm struct {
	MatchExpressions []NodeSelectorRequirement `json:"matchExpressions,omitempty"`
}

type NodeSelectorRequirement struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"`
	Values   []string `json:"values,omitempty"`
}

type PodAffinity struct{}
type PodAntiAffinity struct{}

type TopologySpreadConstraint struct {
	MaxSkew           int            `json:"maxSkew"`
	TopologyKey       string         `json:"topologyKey"`
	WhenUnsatisfiable string         `json:"whenUnsatisfiable"`
	LabelSelector     *LabelSelector `json:"labelSelector,omitempty"`
}

type LabelSelector struct {
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

type PodSecurityContext struct {
	RunAsUser    *int64 `json:"runAsUser,omitempty"`
	RunAsGroup   *int64 `json:"runAsGroup,omitempty"`
	RunAsNonRoot *bool  `json:"runAsNonRoot,omitempty"`
	FSGroup      *int64 `json:"fsGroup,omitempty"`
}

type SecurityContext struct {
	RunAsUser                *int64 `json:"runAsUser,omitempty"`
	RunAsGroup               *int64 `json:"runAsGroup,omitempty"`
	RunAsNonRoot             *bool  `json:"runAsNonRoot,omitempty"`
	AllowPrivilegeEscalation *bool  `json:"allowPrivilegeEscalation,omitempty"`
	ReadOnlyRootFilesystem   *bool  `json:"readOnlyRootFilesystem,omitempty"`
}
