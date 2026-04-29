// Package platform provides runtime configuration applied uniformly to every
// JaisCloud-managed container or pod, irrespective of workload or cloud.
// This includes TLS trust (CA import, PEM bundle), extra volumes, and
// extra environment variables. Executors call ApplyK8s or ApplyDocker to
// materialise these knobs onto their pod spec or docker-run args.
package platform
