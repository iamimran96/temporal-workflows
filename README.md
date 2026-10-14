# temporal-workflows

A local proof of concept of [Temporal](https://temporal.io) for MLOps pipelines. The full Temporal stack runs on a two-node **kind** Kubernetes cluster, backed by PostgreSQL (CloudNativePG), with a **separate Python worker** that runs the workflows.

It shows the Temporal features an ML pipeline needs:

- **Automatic retries**: a flaky training step fails twice and Temporal retries it.
- **Signals and queries**: the pipeline pauses for a human approval signal and reports its progress through a `status` query.
- **Durable state**: kill the worker mid-run and the workflow picks up where it left off.
- **Schedules**: a model-drift check runs every 30 seconds.

## Architecture

```
kind cluster "temporal"
├── temporal-control-plane     control-plane node; host ports 7233 / 8080 map in here
└── temporal-worker            worker node; all workloads below are scheduled here
    ├── namespace cnpg-system
    │   └── cnpg               CloudNativePG operator
    └── namespace temporal
        ├── temporal-pg        PostgreSQL (persistence + visibility databases)
        ├── temporal-*         Temporal server via Helm: frontend, history, matching,
        │                      internal system worker, web UI, admintools
        ├── mlops-worker       OUR worker: own image and Deployment, task queue "mlops-pipeline"
        └── temporal-poc       one-shot Job that runs the POC client (scripts/test.sh)
```

| Endpoint | Address |
|---|---|
| Temporal Web UI | http://127.0.0.1:8080 |
| Temporal gRPC (SDKs, `temporal` CLI) | `127.0.0.1:7233` |

Both ports are exposed through NodePort services (`k8s/nodeports.yaml`) and kind port mappings, so they work no matter which node the pods run on.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/), running, with about 4 GB of memory available
- [kind](https://kind.sigs.k8s.io/), [kubectl](https://kubernetes.io/docs/tasks/tools/) and [Helm 3](https://helm.sh/)
- A bash shell: Linux, macOS, WSL or Git Bash on Windows

You don't need Python on your machine. The worker and the POC client run inside the cluster.

## Quick start

```bash
git clone <this-repo-url>
cd temporal-workflows

bash scripts/deploy.sh     # cluster + Postgres + Temporal + worker (first run pulls images: ~5-10 min)
bash scripts/test.sh       # run the ML pipeline end to end, prints PASS/FAIL
```

Then open the UI at http://127.0.0.1:8080/namespaces/default/workflows.

| Command | What it does |
|---|---|
| `bash scripts/deploy.sh` | Create or refresh everything. Safe to re-run. |
| `bash scripts/deploy.sh worker` | Rebuild and redeploy only the worker, after code changes. |
| `bash scripts/test.sh` | Run the ML pipeline POC as a Kubernetes Job. |
| `bash scripts/schedule.sh create\|describe\|delete` | Manage the 30-second scheduled job. |
| `bash scripts/diagnose.sh` | Dump pods, events and logs when something doesn't come up. |
| `bash scripts/teardown.sh` | Delete the whole cluster. |

## Workflows (`worker/app/`)

### `MlPipelineWorkflow`: training pipeline with human approval

```
validate data → train model → evaluate → [r2 ≥ 0.8?] → wait for "approve" signal → register model
                (fails twice,              │ no
                 retried)                  └→ rejected_low_quality
```

`scripts/test.sh` runs `app/run_poc.py` in the cluster. It starts the workflow, polls the `status` query, sends the `approve` signal and prints the result.

Training and evaluation are simulated, with deterministic results and no ML libraries, so the POC tests Temporal itself. The simulated r2 score falls between 0.75 and 0.95 and depends on the workflow ID, so about one run in four ends as `rejected_low_quality` and `test.sh` reports **FAIL**. That's the pipeline's quality gate working as designed. Run the test again to get a passing run.

**Try durability:** start a run, then kill the worker pod while it's training:

```bash
kubectl --context kind-temporal -n temporal delete pod -l app=mlops-worker
```

The workflow continues as soon as the replacement pod is up.

### `DriftCheckWorkflow`: scheduled every 30 seconds

A simulated model-drift check, started by the Temporal Schedule `drift-check-every-30s`. If one run is still going when the next is due, the next run is skipped.

```bash
bash scripts/schedule.sh create     # create (or update) the schedule
bash scripts/schedule.sh describe   # next run times + results of recent runs
bash scripts/schedule.sh delete     # stop it
```

Each run appears as a workflow named `drift-check-<timestamp>`. To view the schedule in the UI, open http://127.0.0.1:8080/namespaces/default/schedules.

To change the interval, set `SCHEDULE_EVERY_S`, read by `app/schedule.py`, then run `create` again.

### `HelloWorkflow`

The smallest possible workflow, used as a smoke test at the start of `test.sh`.

## Repository layout

```
kind/cluster.yaml        kind cluster: 1 control-plane + 1 worker node, host port mappings
helm/values.yaml         Temporal Helm chart values (Postgres via temporal-pg-rw; no Cassandra/ES/monitoring)
k8s/
  namespace.yaml         "temporal" namespace
  cnpg-cluster.yaml      CloudNativePG cluster, credentials, temporal_visibility database
  nodeports.yaml         NodePort services for the frontend (30233) and Web UI (30880)
  worker.yaml            mlops-worker Deployment
  poc-job.yaml           POC client Job
worker/
  Dockerfile             Python 3.12 image (temporalio SDK)
  app/workflows.py       workflow definitions
  app/activities.py      activities (simulated validate / train / evaluate / register / drift check)
  app/worker.py          worker process that polls the "mlops-pipeline" task queue
  app/run_poc.py         POC client
  app/schedule.py        creates, describes and deletes the 30-second schedule
scripts/                 deploy / test / schedule / diagnose / teardown
```

## Configuration

Optional environment variables for `scripts/deploy.sh`:

| Variable | Default | Notes |
|---|---|---|
| `CLUSTER_NAME` | `temporal` | kind cluster name |
| `NAMESPACE` | `temporal` | Kubernetes namespace for everything |
| `CHART_VERSION` | `0.74.0` | Temporal Helm chart. `helm/values.yaml` uses the 0.x format; charts 1.x changed it. |
| `CNPG_CHART_VERSION` | latest | CloudNativePG operator chart (needs operator ≥ 1.25) |
| `WORKER_IMAGE` | `temporal-worker:dev` | Tag for the worker image |
| `WAIT_TIMEOUT` | `10m` | How long to wait for the Temporal Helm install |

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `'cassandra' is no longer a supported top-level key` | A Temporal chart 1.x was used. Keep `CHART_VERSION=0.74.0`. |
| Temporal pods crash with `Cassandra.Hosts: zero value` | Server images 1.30 and later need `server.configMapsToMount: sprig` and `server.setConfigFilePath: true`. Both are already set in `helm/values.yaml`. |
| `deployment "temporal-worker" ... field is immutable` | The Helm chart owns `temporal-worker` (Temporal's internal worker). Our worker must keep a different name (`mlops-worker`). |
| `helm upgrade` says another operation is in progress | An install was interrupted. Run `helm --kube-context kind-temporal -n temporal uninstall temporal`, then re-run `deploy.sh`. |
| Ports 7233 or 8080 already in use | Stop whatever is using them, or change the `hostPort` values in `kind/cluster.yaml` and recreate the cluster. |

Run `bash scripts/diagnose.sh` for a full dump of pods, events and logs.

## Notes and limitations

- Local POC only: the database password is in plain text, there are 4 history shards, and each service runs a single replica.
- `numHistoryShards` can't be changed after the first install. Run `teardown.sh` and deploy again to change it.
- One Postgres instance. Set `instances: 3` in `k8s/cnpg-cluster.yaml` for high availability.
- Keep the Helm release name `temporal`. `k8s/nodeports.yaml` and the worker's frontend address depend on it.
- kind sets node count and port mappings only when it creates the cluster. To change either, run `teardown.sh` and then `deploy.sh`.
