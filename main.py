import asyncio
import sys
import machine
import models
from data import DataManager
from rich import print
from config_loader import load_test_rig_config
from loguru import logger

logger.remove()
logger.add(sys.stderr,level='INFO')

class TerminateTaskGroup(Exception):
    """Exception raised to terminate a task group."""

async def force_terminate_task_group():
    """Used to force termination of a task group."""
    raise TerminateTaskGroup()

test_rig = machine.TestRig(load_test_rig_config())
test_rig_event_q = asyncio.Queue()

data_manager = DataManager(test_rig.config.data_sinks)

async def flow_tasks(stop_flag: asyncio.Event,
                     on_metrics_update = None):
    print("Hello from st-test-rig!")
    
    async def update_metrics_loop():
        while True:
            await test_rig.update_metrics()
            if on_metrics_update is not None:
                on_metrics_update(test_rig.fetch_flat_metrics())
            await asyncio.sleep(1)
    
    async def report_metrics_loop():
        while True:
            await test_rig.report_metrics()
            await data_manager.handle_data(test_rig._metrics)
            await asyncio.sleep(10)

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(update_metrics_loop())
            tg.create_task(report_metrics_loop())
            tg.create_task(machine.event_handler(test_rig,test_rig_event_q))
            tg.create_task(test_rig.do_supervisory_control(test_rig_event_q))
            # await asyncio.sleep(5)
            # await test_rig_event_q.put(models.SetpointEvent(value=4.2))
            await stop_flag.wait()
            tg.create_task(force_terminate_task_group())
    
    except* TerminateTaskGroup:
        logger.warning('All tasks stopped, shutting down')

    except* Exception as eg:
            # Optional: catch real errors from children
            logger.error(f"Background tasks failed: {eg.exceptions}")

def main():
    stop_flag = asyncio.Event()
    asyncio.run(flow_tasks(stop_flag))

if __name__ == "__main__":
   main()