package platform

import (
	"jaiscloud/internal/k8stypes"
)

// ApplyK8s mutates spec and ctr to inject TLS materializers, platform volumes,
// mounts, and env. Ordering: TLS init containers + volumes + env first, then
// platform extra volumes + env. Fails fast on volume name conflicts.
// Call this after cloud-specific contributions are already on the spec.
func ApplyK8s(spec *k8stypes.PodSpec, ctr *k8stypes.Container, cfg *PlatformConfig) error {
	if cfg == nil {
		return nil
	}

	// ── TLS materializers ─────────────────────────────────────────────────────
	if cfg.TLS.Enabled && len(cfg.TLS.CASources) > 0 {
		// Volumes for all CA sources (needed by both materializers).
		caVols := caVolumes(cfg.TLS.CASources)
		if cfg.TLS.ClientCert != nil {
			caVols = append(caVols, caVolumes([]CASource{*cfg.TLS.ClientCert})...)
		}
		if err := checkVolumeConflicts(spec.Volumes, caVols); err != nil {
			return err
		}
		spec.Volumes = append(spec.Volumes, caVols...)

		for _, m := range materializers {
			// Init container.
			if ic := m.InitContainer(&cfg.TLS, ""); ic != nil {
				mVols := m.Volumes()
				if err := checkVolumeConflicts(spec.Volumes, mVols); err != nil {
					return err
				}
				spec.Volumes = append(spec.Volumes, mVols...)
				spec.InitContainers = append(spec.InitContainers, *ic)
			}
			// Main container env + mounts.
			ctr.Env = MergeEnv(ctr.Env, m.ContainerEnv(&cfg.TLS))
			ctr.VolumeMounts = append(ctr.VolumeMounts, m.VolumeMounts()...)
		}
	}

	// ── Platform extra volumes ────────────────────────────────────────────────
	platformVols := platformVolumes(cfg.Volumes)
	if err := checkVolumeConflicts(spec.Volumes, platformVols); err != nil {
		return err
	}
	spec.Volumes = append(spec.Volumes, platformVols...)

	for _, vs := range cfg.Volumes {
		for _, mnt := range vs.Mounts {
			ctr.VolumeMounts = append(ctr.VolumeMounts, k8stypes.VolumeMount{
				Name:      vs.Name,
				MountPath: mnt.MountPath,
				SubPath:   mnt.SubPath,
				ReadOnly:  mnt.ReadOnly,
			})
		}
	}

	// ── Platform extra env ────────────────────────────────────────────────────
	ctr.Env = MergeEnv(ctr.Env, ExtraEnv(cfg.Env))

	return nil
}

// platformVolumes converts []VolumeSpec to []k8stypes.Volume for the pod spec.
func platformVolumes(specs []VolumeSpec) []k8stypes.Volume {
	vols := make([]k8stypes.Volume, 0, len(specs))
	for _, vs := range specs {
		vols = append(vols, toK8sVolume(vs))
	}
	return vols
}

// toK8sVolume maps a platform VolumeSpec to a k8stypes.Volume wire type.
func toK8sVolume(vs VolumeSpec) k8stypes.Volume {
	v := k8stypes.Volume{Name: vs.Name}
	src := vs.Source
	switch src.Kind {
	case "configMap":
		if src.ConfigMap != nil {
			v.ConfigMap = &k8stypes.ConfigMapVol{Name: src.ConfigMap.Name}
		}
	case "secret":
		if src.Secret != nil {
			v.Secret = &k8stypes.SecretVol{SecretName: src.Secret.Name}
		}
	case "emptyDir":
		v.EmptyDir = &k8stypes.EmptyDirVol{}
		if src.EmptyDir != nil {
			v.EmptyDir.Medium = src.EmptyDir.Medium
			v.EmptyDir.SizeLimit = src.EmptyDir.SizeLimit
		}
	case "pvc":
		if src.PVC != nil {
			v.PVC = &k8stypes.PVCVol{ClaimName: src.PVC.ClaimName, ReadOnly: src.PVC.ReadOnly}
		}
	case "csi":
		if src.CSI != nil {
			v.CSI = &k8stypes.CSIVol{
				Driver:           src.CSI.Driver,
				ReadOnly:         src.CSI.ReadOnly,
				FSType:           src.CSI.FSType,
				VolumeAttributes: src.CSI.VolumeAttributes,
			}
		}
	case "hostPath":
		if src.HostPath != nil {
			v.HostPath = &k8stypes.HostPathVol{Path: src.HostPath.Path, Type: src.HostPath.Type}
		}
	case "projected":
		if src.Projected != nil {
			v.Projected = toK8sProjected(src.Projected)
		}
	}
	return v
}

func toK8sProjected(p *ProjectedSource) *k8stypes.ProjectedVol {
	pv := &k8stypes.ProjectedVol{DefaultMode: p.DefaultMode}
	for _, el := range p.Sources {
		kEl := k8stypes.ProjectionElement{}
		if el.ServiceAccountToken != nil {
			kEl.ServiceAccountToken = &k8stypes.SATokenProjection{
				Audience:          el.ServiceAccountToken.Audience,
				ExpirationSeconds: el.ServiceAccountToken.ExpirationSeconds,
				Path:              el.ServiceAccountToken.Path,
			}
		}
		if el.ConfigMap != nil {
			kEl.ConfigMap = &k8stypes.ConfigMapVol{Name: el.ConfigMap.Name}
		}
		if el.Secret != nil {
			kEl.Secret = &k8stypes.ConfigMapVol{Name: el.Secret.Name}
		}
		if el.DownwardAPI != nil {
			items := make([]k8stypes.DownwardAPIItem, len(el.DownwardAPI.Items))
			for i, it := range el.DownwardAPI.Items {
				items[i] = k8stypes.DownwardAPIItem{Path: it.Path}
				if it.FieldRef != nil {
					items[i].FieldRef = &k8stypes.ObjectFieldSelector{FieldPath: it.FieldRef.FieldPath}
				}
			}
			kEl.DownwardAPI = &k8stypes.DownwardAPIProjection{Items: items}
		}
		pv.Sources = append(pv.Sources, kEl)
	}
	return pv
}
