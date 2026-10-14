#!/usr/bin/env bash
# Quick health dump when something does not come up.
CLUSTER_NAME="${CLUSTER_NAME:-temporal}"
NAMESPACE="${NAMESPACE:-temporal}"
kc() { kubectl --context "kind-${CLUSTER_NAME}" "$@"; }
kc -n "$NAMESPACE" get pods,svc,jobs
echo; echo "--- cnpg"; kc -n "$NAMESPACE" get cluster,database
echo; echo "--- endpoints"; kc -n "$NAMESPACE" get endpoints
echo; echo "--- recent events"; kc -n "$NAMESPACE" get events --sort-by=.lastTimestamp | tail -25
echo; echo "--- worker logs"; kc -n "$NAMESPACE" logs deploy/mlops-worker --tail=30
echo; echo "--- frontend logs"; kc -n "$NAMESPACE" logs deploy/temporal-frontend --tail=30
