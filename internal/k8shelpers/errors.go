package k8shelpers

import "errors"

var (
	ErrJobNotFound        = errors.New("k8shelpers: job not found")
	ErrPodNotScheduled    = errors.New("k8shelpers: pod not yet scheduled")
	ErrUnknownOptOutToken = errors.New("k8shelpers: unknown platform-overlay opt-out token")
)
