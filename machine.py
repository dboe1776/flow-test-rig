import periphs
import models
import asyncio
from dataclasses import asdict
from loguru import logger
from typing import Optional

class TestRig:

    def __init__(self,
                 config:models.TestRigConfig
                 ):
        self.config = config
    
        self.mass: periphs.scale.ADEK30KL = None
        self.flow: periphs.alicat.AlicatFlowController = None
        self.high_dp: periphs.alicat.AlicatDiffPressure = None
        self.low_dp: periphs.alicat.AlicatDiffPressure = None
        
        self._metrics: Optional[models.TestRigDF] = None

        try:
            if config.mock:
                self._load_mock_devices()
            else: 
                self._load_real_devices()
        except Exception as e:
            raise RuntimeError(f'Error in initializing serial deivces: {e}')

    def _load_mock_devices(self):
        mass_serial = periphs.utils.MockSerialDevice(
            response_mapper=periphs.scale.ADEK30KL.mock_command_map,
            name='MockScale'
        )
        alicat_flow_serial = periphs.utils.MockSerialDevice(
            response_mapper=periphs.alicat.AlicatFlowController.mock_command_map,
            name='FlowMockSerial'
        )
        alicat_pressure_serial = periphs.utils.MockSerialDevice(
            response_mapper=periphs.alicat.AlicatDiffPressure.mock_command_map,
            name='PressureMockSerial'
        )

        self.mass = periphs.scale.ADEK30KL(mass_serial)
        self.flow = periphs.alicat.AlicatFlowController(alicat_flow_serial,
                                                                self.config.flow.unit_id)
        self.high_dp = periphs.alicat.AlicatDiffPressure(alicat_pressure_serial,
                                                               self.config.high_dp.unit_id)
        self.low_dp = periphs.alicat.AlicatDiffPressure(alicat_pressure_serial,
                                                              self.config.low_dp.unit_id)

    def _load_real_devices(self):

        # Scale always uses it's own serial device
        mass_serial = periphs.utils.SimpleSerialDevice(
            **asdict(self.config.mass.serial),
            name='ScaleSerial'
        )
        self.mass = periphs.scale.ADEK30KL(mass_serial)

        # A shared alicat serial config may be defined
        if self.config.alicat_shared.serial:
            shared_alicat_serial = periphs.utils.SimpleSerialDevice(
                **asdict(self.config.alicat_shared.serial),
                name='AlicatSharedSerial'
            )
        else:
            shared_alicat_serial = None

        # Each instrument may also have it's own serial device defined,
        # if so, use it; otherwise, default to shared serial
        if self.config.flow.serial:   
            flow_alicat_serial = periphs.utils.SimpleSerialDevice(
                **asdict(self.config.flow.serial),
                name='AlicatFlowSerial'
            )
            self.flow = periphs.alicat.AlicatFlowController(flow_alicat_serial,
                                                                    self.config.flow.unit_id)
        elif shared_alicat_serial:
            self.flow = periphs.alicat.AlicatFlowController(shared_alicat_serial,
                                                                    self.config.flow.unit_id)
        else:
            raise RuntimeError('No serial device available for flow controller')

        if self.config.high_dp.serial:
            high_dp_alicat_serial = periphs.utils.SimpleSerialDevice(
                **asdict(self.config.high_dp.serial),
                name='AlicatHighDPressureSerial'
            )
            self.high_dp = periphs.alicat.AlicatDiffPressure(high_dp_alicat_serial,
                                                                   self.config.high_dp.unit_id)
        elif shared_alicat_serial:
            self.high_dp = periphs.alicat.AlicatDiffPressure(shared_alicat_serial,
                                                                   self.config.high_dp.unit_id)
        else:
            raise RuntimeError('No serial device available for high range pressure sensor')

        if self.config.low_dp.serial:
            low_dp_alicat_serial = periphs.utils.SimpleSerialDevice(
                **asdict(self.config.low_dp.serial),
                name='AlicatLowDPressureSerial'
            )
            self.low_dp = periphs.alicat.AlicatDiffPressure(low_dp_alicat_serial,
                                                                  self.config.low_dp.unit_id)
        elif shared_alicat_serial:
            self.low_dp = periphs.alicat.AlicatDiffPressure(shared_alicat_serial,
                                                                  self.config.low_dp.unit_id)
        else:
            raise RuntimeError('No serial device available for low range pressure sensor')

    async def update_metrics(self):
        try:
            mass_data = await self.mass.fetch_data()
            flow_data = await self.flow.fetch_data()
            low_dp_data = await self.low_dp.fetch_data()
            high_dp_data = await self.high_dp.fetch_data()
            self._metrics = models.TestRigDF(
                mass=mass_data,
                flow=flow_data,
                low_dp=low_dp_data,
                high_dp=high_dp_data
            )
        except Exception as e:
            logger.error(f'Error in update data task: {e}')

    async def report_metrics(self):
        print(self._metrics.flatten())
        await asyncio.sleep(1)