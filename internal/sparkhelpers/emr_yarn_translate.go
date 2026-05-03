package sparkhelpers

import "strings"

const k8sMaster = "k8s://kubernetes.default.svc"

// TranslateEMREC2YarnArgs rewrites `--master yarn --deploy-mode cluster` argv
// into `--master k8s://kubernetes.default.svc --deploy-mode client`.
//
// Handles both `--master yarn` (two-token) and `--master=yarn` (single-token) forms.
// Leaves non-YARN masters and non-cluster deploy modes untouched.
func TranslateEMREC2YarnArgs(args []string) []string {
	if len(args) == 0 {
		return args
	}

	out := make([]string, len(args))
	copy(out, args)

	// Rewrite --master yarn / --master=yarn → --master k8s://...
	for i, arg := range out {
		if arg == "--master" && i+1 < len(out) && strings.EqualFold(out[i+1], "yarn") {
			out[i+1] = k8sMaster
		} else if strings.HasPrefix(arg, "--master=") {
			val := arg[len("--master="):]
			if strings.EqualFold(val, "yarn") {
				out[i] = "--master=" + k8sMaster
			}
		}
	}

	// Rewrite --deploy-mode cluster → --deploy-mode client (only when master was yarn).
	// We check if master is now k8s:// (was rewritten) and deploy-mode is cluster.
	masterIsK8s := false
	for i, arg := range out {
		if arg == "--master" && i+1 < len(out) && strings.HasPrefix(out[i+1], "k8s://") {
			masterIsK8s = true
		} else if strings.HasPrefix(arg, "--master=k8s://") {
			masterIsK8s = true
		}
		_ = i
	}

	if masterIsK8s {
		for i, arg := range out {
			if arg == "--deploy-mode" && i+1 < len(out) && strings.EqualFold(out[i+1], "cluster") {
				out[i+1] = "client"
			} else if strings.HasPrefix(arg, "--deploy-mode=") {
				val := arg[len("--deploy-mode="):]
				if strings.EqualFold(val, "cluster") {
					out[i] = "--deploy-mode=client"
				}
			}
		}
	}

	return out
}
