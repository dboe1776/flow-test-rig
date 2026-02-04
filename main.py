import asyncio
import sys
from config_loader import load_test_rig_config
from loguru import logger
from machine import TestRig

logger.remove()
logger.add(sys.stderr,level='INFO')

async def main():
    print("Hello from st-test-rig!")
    test_rig = TestRig(load_test_rig_config())
    
    async def update_metrics_loop():
        for i in range(10):
            await test_rig.update_metrics()
            await asyncio.sleep(1)
    
    async def report_metrics_loop():
        for i in range(10):
            await test_rig.report_metrics()
            await asyncio.sleep(1)

    async def ramp_setpoint_loop():
        for i in range(5):
            await test_rig.flow.write_setpoint(value=i)
            await asyncio.sleep(1.5)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(update_metrics_loop())
        tg.create_task(report_metrics_loop())
        tg.create_task(ramp_setpoint_loop())



if __name__ == "__main__":
   asyncio.run(main())
