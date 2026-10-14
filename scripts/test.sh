#!/usr/bin/env bash
# Run the POC: an in-cluster Job starts MlPipelineWorkflow, queries its status,
# sends the approval signal and prints the result. Exit code 0 = PASS.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-temporal}"
NAMESPACE="${NAMESPACE:-temporal}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
kc() { kubectl --context "$KUBE_CONTEXT" "$@"; }

echo "==> Worker status"
kc -n "$NAMESPACE" get pods -l app=mlops-worker

echo "==> Starting POC job"
kc -n "$NAMESPACE" delete job temporal-poc --ignore-not-found >/dev/null
kc apply -f "$REPO_DIR/k8s/poc-job.yaml" >/dev/null

# Wait for either success or failure.
for _ in $(seq 1 90); do
  succeeded="$(kc -n "$NAMESPACE" get job temporal-poc -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
  failed="$(kc -n "$NAMESPACE" get job temporal-poc -o jsonpath='{.status.failed}' 2>/dev/null || true)"
  [[ "$succeeded" == "1" || -n "$failed" ]] && break
  sleep 2
done

kc -n "$NAMESPACE" logs job/temporal-poc || true
echo "==> Worker logs (last 20 lines)"
kc -n "$NAMESPACE" logs deploy/mlops-worker --tail=20 || true

if [[ "${succeeded:-}" == "1" ]]; then
  echo "==> POC PASSED. Inspect the run at http://127.0.0.1:8080/namespaces/default/workflows"
else
  echo "==> POC FAILED" >&2
  exit 1
fi
