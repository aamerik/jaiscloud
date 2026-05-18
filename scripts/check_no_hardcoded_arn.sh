#!/usr/bin/env bash
# Fail the build if any internal/aws/ Go production file contains "arn:aws:" in a string literal.
# Exempts: internal/aws/arn/, internal/config/config.go, *_test.go files, and comment-only lines.
set -euo pipefail

violations=$(grep -rn '"arn:aws:\|`arn:aws:' internal/aws/ \
  --include='*.go' \
  | grep -v '_test\.go:' \
  | grep -v 'internal/aws/arn/' \
  | grep -v 'internal/config/config.go' \
  | grep -v '^\s*//' \
  | grep -v '//.*arn:aws:' \
  | grep -v 'nolint:hardcoded-arn' \
  | grep -v '::aws:policy/' \
  | grep -v '::aws:policy/' \
  || true)

if [[ -n "$violations" ]]; then
  echo "Hardcoded ARN literals found — use nr.ResourceID() or the arn package:"
  echo "$violations"
  exit 1
fi
echo "No hardcoded ARN literals found."
