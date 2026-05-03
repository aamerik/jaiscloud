package sparkhelpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTranslateEMREC2YarnArgs(t *testing.T) {
	tests := []struct {
		name  string
		input []string
		want  []string
	}{
		{
			name:  "two-token yarn to k8s",
			input: []string{"--master", "yarn", "--deploy-mode", "cluster", "--class", "Main", "app.jar"},
			want:  []string{"--master", k8sMaster, "--deploy-mode", "client", "--class", "Main", "app.jar"},
		},
		{
			name:  "single-token --master=yarn form",
			input: []string{"--master=yarn", "--deploy-mode=cluster", "app.jar"},
			want:  []string{"--master=" + k8sMaster, "--deploy-mode=client", "app.jar"},
		},
		{
			name:  "already k8s master untouched",
			input: []string{"--master", "k8s://already", "--deploy-mode", "client"},
			want:  []string{"--master", "k8s://already", "--deploy-mode", "client"},
		},
		{
			name:  "local master untouched",
			input: []string{"--master", "local[*]", "--deploy-mode", "client", "app.jar"},
			want:  []string{"--master", "local[*]", "--deploy-mode", "client", "app.jar"},
		},
		{
			name:  "yarn without deploy-mode",
			input: []string{"--master", "yarn", "app.jar"},
			want:  []string{"--master", k8sMaster, "app.jar"},
		},
		{
			name:  "yarn with non-cluster deploy-mode left alone",
			input: []string{"--master", "yarn", "--deploy-mode", "client"},
			want:  []string{"--master", k8sMaster, "--deploy-mode", "client"},
		},
		{
			name:  "YARN uppercase",
			input: []string{"--master", "YARN", "--deploy-mode", "cluster"},
			want:  []string{"--master", k8sMaster, "--deploy-mode", "client"},
		},
		{
			name:  "empty args",
			input: []string{},
			want:  []string{},
		},
		{
			name:  "nil args",
			input: nil,
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TranslateEMREC2YarnArgs(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}
