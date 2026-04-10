#!/usr/bin/env bash
# deploy.sh — one-click build + deploy JaisCloud + Postgres to local Kubernetes
#
# Requirements:
#   - Docker Desktop with Kubernetes enabled
#   - kubectl pointing at docker-desktop context
#
# Usage:
#   ./deploy/deploy.sh            # build image + apply manifests
#   ./deploy/deploy.sh --delete   # remove all pods/services; keep PVCs (data survives)
#   ./deploy/deploy.sh --reset    # wipe ALL persisted data (PVCs) AND remove everything

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
K8S_DIR="$REPO_ROOT/deploy/k8s"
IMAGE="jaiscloud:latest"
NAMESPACE="jaiscloud"

# PVCs that hold persisted data — these are NOT deleted by --delete
PVCS=("postgres-pvc" "jaiscloud-blobs-pvc")

# ── helpers ──────────────────────────────────────────────────────────────────

info()  { printf '\033[1;34m[INFO]\033[0m  %s\n' "$*"; }
ok()    { printf '\033[1;32m[OK]\033[0m    %s\n' "$*"; }
warn()  { printf '\033[1;33m[WARN]\033[0m  %s\n' "$*"; }
die()   { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

require() {
  command -v "$1" &>/dev/null || die "'$1' not found — please install it first"
}

wait_for_rollout() {
  local resource=$1
  info "Waiting for $resource to be ready..."
  kubectl rollout status "$resource" -n "$NAMESPACE" --timeout=120s
}

# ── --delete: remove workloads, keep PVCs ────────────────────────────────────

if [[ "${1:-}" == "--delete" ]]; then
  info "Removing deployments and services (persisted data is preserved)..."
  kubectl delete deployment postgres jaiscloud -n "$NAMESPACE" --ignore-not-found
  kubectl delete service  postgres jaiscloud -n "$NAMESPACE" --ignore-not-found
  kubectl delete configmap jaiscloud-config   -n "$NAMESPACE" --ignore-not-found
  kubectl delete secret   postgres-secret     -n "$NAMESPACE" --ignore-not-found
  ok "Workloads removed. PVCs retained:"
  for pvc in "${PVCS[@]}"; do
    kubectl get pvc "$pvc" -n "$NAMESPACE" --no-headers 2>/dev/null \
      && echo "    pvc/$pvc" || true
  done
  echo ""
  echo "  Data is still on disk. Re-deploy with: $0"
  echo "  To wipe data too, run:                 $0 --reset"
  exit 0
fi

# ── --reset: wipe PVCs AND remove everything ─────────────────────────────────

if [[ "${1:-}" == "--reset" ]]; then
  warn "This will permanently delete ALL persisted data (postgres DB + blob files)."
  read -rp "Are you sure? [y/N] " yn
  [[ "${yn,,}" == "y" ]] || { info "Aborted."; exit 0; }

  info "Scaling deployments to zero (to release PVC bindings)..."
  kubectl scale deployment postgres jaiscloud --replicas=0 -n "$NAMESPACE" \
    --ignore-not-found 2>/dev/null || true
  sleep 3

  info "Deleting PVCs..."
  for pvc in "${PVCS[@]}"; do
    kubectl delete pvc "$pvc" -n "$NAMESPACE" --ignore-not-found
  done

  info "Deleting namespace $NAMESPACE..."
  kubectl delete namespace "$NAMESPACE" --ignore-not-found

  ok "All resources and persisted data deleted."
  echo ""
  echo "  Re-deploy with: $0"
  exit 0
fi

# ── pre-flight ───────────────────────────────────────────────────────────────

require docker
require kubectl

CONTEXT=$(kubectl config current-context 2>/dev/null || true)
if [[ "$CONTEXT" != "docker-desktop" ]]; then
  warn "Current kubectl context is '$CONTEXT', not 'docker-desktop'."
  warn "If you have a different local cluster (minikube, kind, rancher-desktop),"
  warn "make sure it's the active context before continuing."
  read -rp "Continue anyway? [y/N] " yn
  [[ "${yn,,}" == "y" ]] || exit 1
fi

# ── build image ──────────────────────────────────────────────────────────────

info "Building Docker image: $IMAGE"
docker build -t "$IMAGE" "$REPO_ROOT"
ok "Image built."

# ── apply manifests ──────────────────────────────────────────────────────────

info "Applying Kubernetes manifests..."
kubectl apply -f "$K8S_DIR/namespace.yaml"
kubectl apply -f "$K8S_DIR/postgres.yaml"
kubectl apply -f "$K8S_DIR/jaiscloud.yaml"

# ── wait for postgres ─────────────────────────────────────────────────────────

wait_for_rollout "deployment/postgres"
ok "Postgres is ready."

# ── wait for jaiscloud ────────────────────────────────────────────────────────

wait_for_rollout "deployment/jaiscloud"
ok "JaisCloud is ready."

# ── smoke test ────────────────────────────────────────────────────────────────

info "Smoke-testing health endpoint..."
for i in $(seq 1 10); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:4566/_jaiscloud/health || true)
  if [[ "$STATUS" == "200" ]]; then
    ok "Health check passed (HTTP 200)."
    break
  fi
  if [[ $i -eq 10 ]]; then
    warn "Health check returned HTTP $STATUS after 10 attempts — check logs with:"
    warn "  kubectl logs -n $NAMESPACE deployment/jaiscloud"
  fi
  sleep 2
done

# ── summary ───────────────────────────────────────────────────────────────────

echo ""
ok "Deployment complete."
echo ""
echo "  Endpoint : http://localhost:4566"
echo "  Metrics  : http://localhost:4566/metrics"
echo "  Health   : http://localhost:4566/_jaiscloud/health"
echo ""
echo "  Logs     : kubectl logs -n $NAMESPACE deployment/jaiscloud -f"
echo ""
echo "  Remove workloads (keep data) : $0 --delete"
echo "  Wipe everything incl. data   : $0 --reset"
echo ""
echo "  NOTE: On non-Docker-Desktop clusters (minikube, kind) the LoadBalancer"
echo "  external IP may stay <pending>. Use port-forward instead:"
echo "    kubectl port-forward -n $NAMESPACE svc/jaiscloud 4566:4566"
echo ""
