"""Workflows for the POC.

MlPipelineWorkflow: validate -> train (flaky, retried) -> evaluate -> [wait for human
approval signal] -> register. Exposes a `status` query and an `approve` signal.
"""
import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from app.activities import check_data_drift, evaluate_model, register_model, train_model, validate_data


@dataclass
class PipelineInput:
    dataset: str = "house-prices"
    model_name: str = "house-price-regressor"
    min_r2: float = 0.8
    require_approval: bool = True
    approval_timeout_s: int = 300
    simulate_flaky: bool = True


@workflow.defn
class HelloWorkflow:
    """Smallest possible workflow, handy for smoke tests."""

    @workflow.run
    async def run(self, name: str) -> str:
        return f"Hello, {name}!"


@workflow.defn
class DriftCheckWorkflow:
    """Periodic job, started by the Temporal Schedule from app/schedule.py."""

    @workflow.run
    async def run(self, model_name: str) -> dict:
        return await workflow.execute_activity(
            check_data_drift,
            args=[model_name, workflow.now().isoformat(timespec="seconds")],
            start_to_close_timeout=timedelta(seconds=30),
        )


@workflow.defn
class MlPipelineWorkflow:
    def __init__(self) -> None:
        self._status = "created"
        self._approved: Optional[bool] = None

    @workflow.signal
    def approve(self, approved: bool = True) -> None:
        self._approved = approved

    @workflow.query
    def status(self) -> str:
        return self._status

    @workflow.run
    async def run(self, inp: PipelineInput) -> dict:
        retry = RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2.0, maximum_attempts=5)
        opts = dict(start_to_close_timeout=timedelta(minutes=2), retry_policy=retry)

        self._status = "validating"
        data = await workflow.execute_activity(validate_data, inp.dataset, **opts)

        self._status = "training"
        trained = await workflow.execute_activity(train_model, args=[inp.dataset, inp.simulate_flaky], **opts)

        self._status = "evaluating"
        metrics = await workflow.execute_activity(evaluate_model, args=[inp.dataset, trained["artifact"]], **opts)

        if metrics["r2"] < inp.min_r2:
            self._status = "rejected_low_quality"
            return {"status": self._status, "metrics": metrics, "data": data, "training": trained}

        if inp.require_approval:
            self._status = "awaiting_approval"
            try:
                await workflow.wait_condition(
                    lambda: self._approved is not None,
                    timeout=timedelta(seconds=inp.approval_timeout_s),
                )
            except asyncio.TimeoutError:
                self._status = "approval_timed_out"
                return {"status": self._status, "metrics": metrics}
            if not self._approved:
                self._status = "rejected_by_reviewer"
                return {"status": self._status, "metrics": metrics}

        self._status = "registering"
        registered = await workflow.execute_activity(
            register_model, args=[inp.model_name, trained["artifact"], metrics["r2"]], **opts
        )
        self._status = "completed"
        return {"status": self._status, "data": data, "training": trained, "metrics": metrics, "registered": registered}
