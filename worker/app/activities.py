"""Activities for the POC ML pipeline.

Training/evaluation are simulated (deterministic, no ML deps) so the POC exercises
Temporal itself: retries, timeouts, signals, queries and durable state.
"""
import asyncio
import hashlib

from temporalio import activity
from temporalio.exceptions import ApplicationError


def _score(*parts: str) -> float:
    """Deterministic pseudo-metric in [0, 1) derived from the inputs."""
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return int(digest[:8], 16) / 0x100000000


@activity.defn
async def validate_data(dataset: str) -> dict:
    activity.logger.info("validating dataset %s", dataset)
    await asyncio.sleep(1)
    if not dataset:
        # Non-retryable: bad input will never succeed on retry.
        raise ApplicationError("dataset name is empty", non_retryable=True)
    return {"dataset": dataset, "rows": 20640, "valid": True}


@activity.defn
async def train_model(dataset: str, simulate_flaky: bool) -> dict:
    info = activity.info()
    activity.logger.info("training on %s (attempt %d)", dataset, info.attempt)
    await asyncio.sleep(2)
    if simulate_flaky and info.attempt <= 2:
        # Demonstrates Temporal's automatic activity retries.
        raise RuntimeError(f"simulated transient failure on attempt {info.attempt}")
    return {"artifact": f"s3://models/{dataset}/{info.workflow_id}.pkl", "attempts_needed": info.attempt}


@activity.defn
async def evaluate_model(dataset: str, artifact: str) -> dict:
    activity.logger.info("evaluating %s", artifact)
    await asyncio.sleep(1)
    r2 = round(0.75 + 0.2 * _score(dataset, artifact), 4)
    return {"r2": r2}


@activity.defn
async def check_data_drift(model_name: str, checked_at: str) -> dict:
    activity.logger.info("checking drift for %s at %s", model_name, checked_at)
    await asyncio.sleep(1)
    drift = round(_score(model_name, checked_at) * 0.3, 4)
    return {"model": model_name, "checked_at": checked_at, "drift": drift, "drifted": drift > 0.2}


@activity.defn
async def register_model(model_name: str, artifact: str, r2: float) -> dict:
    info = activity.info()
    activity.logger.info("registering %s", model_name)
    await asyncio.sleep(1)
    version = "v" + hashlib.sha1(info.workflow_id.encode()).hexdigest()[:6]
    return {"model": model_name, "version": version, "artifact": artifact, "r2": r2}
