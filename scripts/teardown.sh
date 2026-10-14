#!/usr/bin/env bash
# Delete the kind cluster (and with it Temporal, PostgreSQL and the worker).
set -euo pipefail
CLUSTER_NAME="${CLUSTER_NAME:-temporal}"
kind delete cluster --name "$CLUSTER_NAME"
