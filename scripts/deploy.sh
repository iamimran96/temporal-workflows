#!/usr/bin/env bash
# Create a local kind cluster, deploy Temporal (Helm, PostgreSQL-backed) and the
# separate flow worker. Safe to re-run.
#
#   Temporal frontend (gRPC): 127.0.0.1:7233
#   Temporal Web UI:          http://127.0.0.1:8080
#
# Configuration (environment variables, all optional):
#   CLUSTER_NAME    kind cluster name                    (default: temporal)
#   NAMESPACE       namespace for everything             (default: temporal)
#   RELEASE_NAME    Helm release name (keep "temporal": nodeports.yaml/worker.yaml rely on it)
#   CHART_VERSION   temporalio/temporal chart version    (default: 0.74.0; helm/values.yaml uses the 0.x format)
#   CNPG_CHART_VERSION  cloudnative-pg operator chart version (default: latest)
#   WORKER_IMAGE    worker image tag                     (default: temporal-worker:dev)
#   WAIT_TIMEOUT    how long to wait for Temporal        (default: 10m)
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-temporal}"
NAMESPACE="${NAMESPACE:-temporal}"
RELEASE_NAME="${RELEASE_NAME:-temporal}"
CHART_VERSION="${CHART_VERSION:-0.74.0}"
CNPG_CHART_VERSION="${CNPG_CHART_VERSION:-}"
PG_CLUSTER="temporal-pg"
WORKER_IMAGE="${WORKER_IMAGE:-temporal-worker:dev}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-10m}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mWARN:\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }
kc()   { kubectl --context "$KUBE_CONTEXT" "$@"; }

check_prereqs() {
  log "Checking prerequisites"
  local missing=()
  for tool in docker kind kubectl helm; do
    command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
  done
  [[ ${#missing[@]} -eq 0 ]] || fail "Missing tools: ${missing[*]}"
  docker info >/dev/null 2>&1 || fail "Docker daemon is not running. Start Docker Desktop and retry."
}

create_cluster() {
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    log "kind cluster '$CLUSTER_NAME' already exists, reusing it"
  else
    log "Creating kind cluster '$CLUSTER_NAME'"
    kind create cluster --config "$REPO_DIR/kind/cluster.yaml" --name "$CLUSTER_NAME"
  fi
  kc cluster-info >/dev/null || fail "Cannot reach the cluster with context $KUBE_CONTEXT"
}

deploy_postgres() {
  log "Installing CloudNativePG operator"
  helm repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true
  helm repo update cnpg >/dev/null
  local cnpg_version_args=()
  [[ -n "$CNPG_CHART_VERSION" ]] && cnpg_version_args=(--version "$CNPG_CHART_VERSION")
  helm upgrade --install cnpg cnpg/cloudnative-pg \
    --kube-context "$KUBE_CONTEXT" -n cnpg-system --create-namespace \
    "${cnpg_version_args[@]}" --wait --timeout 5m
  kc wait --for=condition=Established crd/clusters.postgresql.cnpg.io crd/databases.postgresql.cnpg.io --timeout=120s

  log "Creating CNPG cluster '$PG_CLUSTER' and the Temporal databases"
  kc apply -f "$REPO_DIR/k8s/namespace.yaml"
  kc apply -f "$REPO_DIR/k8s/cnpg-cluster.yaml"
  kc -n "$NAMESPACE" wait --for=condition=Ready "cluster/$PG_CLUSTER" --timeout=600s
  local applied=""
  for _ in $(seq 1 60); do
    applied="$(kc -n "$NAMESPACE" get database temporal-visibility -o jsonpath='{.status.applied}' 2>/dev/null || true)"
    [[ "$applied" == "true" ]] && break
    sleep 5
  done
  [[ "$applied" == "true" ]] || fail "Database 'temporal_visibility' was not applied by CNPG (needs operator >= 1.25). See: kubectl -n $NAMESPACE describe database temporal-visibility"
}

deploy_temporal() {
  log "Installing Temporal via Helm (release '$RELEASE_NAME')"
  helm repo add temporal https://go.temporal.io/helm-charts >/dev/null 2>&1 || true
  helm repo update temporal >/dev/null
  local version_args=()
  [[ -n "$CHART_VERSION" ]] && version_args=(--version "$CHART_VERSION")
  helm upgrade --install "$RELEASE_NAME" temporal/temporal \
    --kube-context "$KUBE_CONTEXT" -n "$NAMESPACE" \
    -f "$REPO_DIR/helm/values.yaml" "${version_args[@]}" \
    --timeout "$WAIT_TIMEOUT" --wait
  log "Exposing frontend and Web UI via NodePort"
  kc apply -f "$REPO_DIR/k8s/nodeports.yaml"
  local ep
  for svc in temporal-frontend-nodeport temporal-web-nodeport; do
    ep="$(kc -n "$NAMESPACE" get endpoints "$svc" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || true)"
    [[ -n "$ep" ]] || warn "$svc has no endpoints (chart labels differ?). Use: kubectl -n $NAMESPACE port-forward svc/${RELEASE_NAME}-frontend 7233:7233"
  done
}

ensure_default_namespace() {
  log "Ensuring Temporal namespace 'default' exists"
  local admin="deploy/${RELEASE_NAME}-admintools"
  if kc -n "$NAMESPACE" exec "$admin" -- temporal operator namespace describe --namespace default >/dev/null 2>&1; then
    return
  fi
  kc -n "$NAMESPACE" exec "$admin" -- temporal operator namespace create --namespace default --retention 72h 2>/dev/null \
    || kc -n "$NAMESPACE" exec "$admin" -- tctl --ns default namespace register --retention 3 2>/dev/null \
    || warn "Could not verify/create namespace 'default'; check the Web UI."
}

deploy_worker() {
  log "Building worker image $WORKER_IMAGE"
  docker build -t "$WORKER_IMAGE" "$REPO_DIR/worker"
  log "Loading image into kind"
  kind load docker-image "$WORKER_IMAGE" --name "$CLUSTER_NAME"
  kc apply -f "$REPO_DIR/k8s/worker.yaml"
  kc -n "$NAMESPACE" rollout restart deploy/mlops-worker >/dev/null
  kc -n "$NAMESPACE" rollout status deploy/mlops-worker --timeout=180s
}

main() {
  check_prereqs
  create_cluster
  deploy_postgres
  deploy_temporal
  ensure_default_namespace
  deploy_worker
  log "Done."
  echo "  Web UI:   http://127.0.0.1:8080"
  echo "  gRPC:     127.0.0.1:7233"
  echo "  Run POC:  bash scripts/test.sh"
}

# `deploy.sh worker` rebuilds and redeploys only the worker.
case "${1:-all}" in
  worker) check_prereqs; deploy_worker ;;
  all)    main ;;
  *)      fail "Usage: $0 [all|worker]" ;;
esac
