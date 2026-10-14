"""Manage the Temporal Schedule that runs DriftCheckWorkflow every SCHEDULE_EVERY_S seconds.

    python -m app.schedule create     # create (or update) the schedule
    python -m app.schedule describe   # show the next runs and recent results
    python -m app.schedule delete     # remove it

Run in-cluster via scripts/schedule.sh, or from the host: TEMPORAL_ADDRESS=localhost:7233 python -m app.schedule create
"""
import asyncio
import os
import sys
from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleUpdate,
)

from app.workflows import DriftCheckWorkflow

SCHEDULE_ID = "drift-check-every-30s"


def build_schedule(task_queue: str, every_s: int) -> Schedule:
    return Schedule(
        # Temporal appends the scheduled time to this id, so each run is a separate workflow.
        action=ScheduleActionStartWorkflow(
            DriftCheckWorkflow.run, "house-price-regressor", id="drift-check", task_queue=task_queue
        ),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(seconds=every_s))]),
        # A run that is still going when the next one is due is not overlapped.
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )


async def main(cmd: str) -> int:
    address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    task_queue = os.getenv("TASK_QUEUE", "mlops-pipeline")
    every_s = int(os.getenv("SCHEDULE_EVERY_S", "30"))
    client = await Client.connect(address, namespace=namespace)
    handle = client.get_schedule_handle(SCHEDULE_ID)

    if cmd == "create":
        schedule = build_schedule(task_queue, every_s)
        try:
            await client.create_schedule(SCHEDULE_ID, schedule)
            print(f"created schedule {SCHEDULE_ID}: DriftCheckWorkflow every {every_s}s")
        except ScheduleAlreadyRunningError:
            await handle.update(lambda _: ScheduleUpdate(schedule=schedule))
            print(f"updated schedule {SCHEDULE_ID}: DriftCheckWorkflow every {every_s}s")
    elif cmd == "describe":
        desc = await handle.describe()
        info = desc.info
        print(f"schedule {SCHEDULE_ID}: {info.num_actions} runs so far, paused={desc.schedule.state.paused}")
        for t in info.next_action_times[:3]:
            print(f"  next run:   {t.isoformat(timespec='seconds')}")
        for action in info.recent_actions[-5:]:
            wf_id = action.action.workflow_id
            result = await client.get_workflow_handle(wf_id).result()
            print(f"  recent run: {wf_id} -> drift={result['drift']} drifted={result['drifted']}")
    elif cmd == "delete":
        await handle.delete()
        print(f"deleted schedule {SCHEDULE_ID}")
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "")))
