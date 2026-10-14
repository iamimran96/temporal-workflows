"""POC client: start the pipeline, watch it via a query, approve via a signal, print the result.

Runs in-cluster as k8s/poc-job.yaml, or from the host: TEMPORAL_ADDRESS=localhost:7233 python -m app.run_poc
"""
import asyncio
import json
import os
import sys
import time

from temporalio.client import Client

from app.workflows import HelloWorkflow, MlPipelineWorkflow, PipelineInput


async def main() -> int:
    address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    task_queue = os.getenv("TASK_QUEUE", "mlops-pipeline")
    client = await Client.connect(address, namespace=namespace)
    print(f"connected to {address} (namespace={namespace})")

    # 1) Smoke test
    hello = await client.execute_workflow(
        HelloWorkflow.run, "temporal", id=f"hello-{int(time.time())}", task_queue=task_queue
    )
    print(f"[hello] {hello}")

    # 2) ML pipeline with retries, query and signal
    wf_id = f"ml-pipeline-{int(time.time())}"
    handle = await client.start_workflow(
        MlPipelineWorkflow.run, PipelineInput(), id=wf_id, task_queue=task_queue
    )
    print(f"[pipeline] started workflow_id={wf_id} run_id={handle.result_run_id}")

    last = None
    deadline = time.time() + 180
    while time.time() < deadline:
        status = await handle.query(MlPipelineWorkflow.status)
        if status != last:
            print(f"[pipeline] status -> {status}")
            last = status
        if status == "awaiting_approval":
            await handle.signal(MlPipelineWorkflow.approve, True)
            print("[pipeline] sent approve signal")
            break
        if status in ("completed", "rejected_low_quality"):
            break
        await asyncio.sleep(1)

    result = await handle.result()
    print("[pipeline] result:")
    print(json.dumps(result, indent=2))
    ok = result.get("status") == "completed"
    print("POC RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
