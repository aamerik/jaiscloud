package platform

// VolumeSpec describes a volume and its mounts injected into every managed pod.
type VolumeSpec struct {
	Name   string       `json:"name"`
	Source VolumeSource `json:"source"`
	Mounts []MountSpec  `json:"mounts"`
}

// VolumeSource is a discriminated union over K8s volume kinds.
// Exactly one pointer field must be non-nil, matching Kind.
type VolumeSource struct {
	Kind      string           `json:"kind"` // "secret"|"configMap"|"emptyDir"|"projected"|"pvc"|"csi"|"hostPath"
	Secret    *SecretSource    `json:"secret,omitempty"`
	ConfigMap *ConfigMapSource `json:"configMap,omitempty"`
	EmptyDir  *EmptyDirSource  `json:"emptyDir,omitempty"`
	Projected *ProjectedSource `json:"projected,omitempty"`
	PVC       *PVCSource       `json:"pvc,omitempty"`
	CSI       *CSISource       `json:"csi,omitempty"`
	HostPath  *HostPathSource  `json:"hostPath,omitempty"`
}

type SecretSource struct {
	Name        string      `json:"name"`
	DefaultMode *int32      `json:"defaultMode,omitempty"`
	Items       []KeyToPath `json:"items,omitempty"`
	Optional    *bool       `json:"optional,omitempty"`
}

type ConfigMapSource struct {
	Name        string      `json:"name"`
	DefaultMode *int32      `json:"defaultMode,omitempty"`
	Items       []KeyToPath `json:"items,omitempty"`
	Optional    *bool       `json:"optional,omitempty"`
}

type EmptyDirSource struct {
	Medium    string `json:"medium,omitempty"`
	SizeLimit string `json:"sizeLimit,omitempty"`
}

type ProjectedSource struct {
	Sources     []ProjectionElement `json:"sources"`
	DefaultMode *int32              `json:"defaultMode,omitempty"`
}

type ProjectionElement struct {
	ServiceAccountToken *SATokenProjection     `json:"serviceAccountToken,omitempty"`
	Secret              *ConfigMapSource       `json:"secret,omitempty"`
	ConfigMap           *ConfigMapSource       `json:"configMap,omitempty"`
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

type PVCSource struct {
	ClaimName string `json:"claimName"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type CSISource struct {
	Driver           string            `json:"driver"`
	ReadOnly         bool              `json:"readOnly,omitempty"`
	FSType           string            `json:"fsType,omitempty"`
	VolumeAttributes map[string]string `json:"volumeAttributes,omitempty"`
}

type HostPathSource struct {
	Path string `json:"path"`
	Type string `json:"type,omitempty"`
}

type KeyToPath struct {
	Key  string `json:"key"`
	Path string `json:"path"`
	Mode *int32 `json:"mode,omitempty"`
}

type MountSpec struct {
	MountPath string `json:"mountPath"`
	SubPath   string `json:"subPath,omitempty"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}
