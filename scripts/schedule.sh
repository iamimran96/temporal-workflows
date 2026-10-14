#!/usr/bin/env bash
# Manage the Temporal Schedule that runs DriftCheckWorkflow every 30s.
# Runs app/schedule.py inside the worker pod, so no local Python is needed.
#
#   bash scripts/schedule.sh create     # create/update the schedule
#   bash scripts/schedule.sh describe   # next runs + recent results
#   bash scripts/schedule.sh delete     # remove it
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-temporal}"
NAMESPACE="${NAMESPACE:-temporal}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"

kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" exec deploy/mlops-worker -- \
  python -m app.schedule "${1:-describe}"
