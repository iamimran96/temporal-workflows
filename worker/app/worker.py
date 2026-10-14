"""Worker process: polls the task queue and runs the workflows/activities above."""
import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from app.activities import check_data_drift, evaluate_model, register_model, train_model, validate_data
from app.workflows import DriftCheckWorkflow, HelloWorkflow, MlPipelineWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("worker")


async def connect(address: str, namespace: str) -> Client:
    # The pod may start before the Temporal frontend is ready; retry instead of crashing.
    while True:
        try:
            return await Client.connect(address, namespace=namespace)
        except Exception as exc:  # noqa: BLE001
            log.warning("Temporal not reachable at %s (%s); retrying in 5s", address, exc)
            await asyncio.sleep(5)


async def main() -> None:
    address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    task_queue = os.getenv("TASK_QUEUE", "mlops-pipeline")

    client = await connect(address, namespace)
    log.info("connected to %s (namespace=%s), polling task queue %s", address, namespace, task_queue)
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[HelloWorkflow, MlPipelineWorkflow, DriftCheckWorkflow],
        activities=[validate_data, train_model, evaluate_model, register_model, check_data_drift],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
