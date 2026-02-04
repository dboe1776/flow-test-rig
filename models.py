import json
import datetime as dt
from dataclasses import dataclass, field, asdict
from typing import Optional
from periphs import alicat, scale

@dataclass(kw_only=True)
class TestRigDF:
    time: float = field(
        default_factory=dt.datetime.now(dt.UTC).timestamp
        )    
    mass: Optional[scale.ADEK30KL_DF] = None
    flow: Optional[alicat.AlicatMassFlowDF] = None
    low_dp: Optional[alicat.AlicatBaseDF] = None
    high_dp: Optional[alicat.AlicatBaseDF] = None
    
    def flatten(self):
        mass = self.mass.flatten(prefix='mass',exclude=['time','header','unit'])
        flow = self.flow.flatten(prefix='flow',exclude=['time','unit_id'])
        low_dp = self.low_dp.flatten(prefix='low_dp',exclude=['time','unit_id'])
        high_dp = self.high_dp.flatten(prefix='high_dp',exclude=['time','unit_id'])
        return {'time':self.time,**mass,**flow,**low_dp,**high_dp}

@dataclass
class SerialConfig:
    """Serial port settings (used for Alicat devices and scale)."""
    port: str = "/dev/ttyUSB0"
    baudrate: int = 19200
    bytesize: int = 8
    parity: str = "N"
    stopbits: int = 1
    timeout: float = 1.0           # seconds
    query_timeout: float = 1.5

@dataclass
class ScaleConfig:
    """Scale (load cell) configuration."""
    serial: SerialConfig
    units: str = "g"

@dataclass
class AlicatSharedSerial:
    """Optional shared serial connection for multi-drop / multiplexed Alicat devices.
    If defined, Alicat devices without their own 'serial' block will use this one.
    """
    serial: Optional[SerialConfig] = None

@dataclass
class AlicatConfig:
    """Base for all Alicat instruments."""
    full_scale_min: float
    full_scale_max: float
    unit_id: str = "A"                     # 'A'–'Z' for multi-drop addressing
    pressure_unit: str = "PSI"
    serial: Optional[SerialConfig] = None  # device-specific override

@dataclass
class FlowControlConfig(AlicatConfig):
    """Mass flow controller/meter."""
    flow_unit: str = "SLPM"

@dataclass
class DiffPressConfig(AlicatConfig):
    """Differential pressure sensor."""
    pass  # extend later if needed (e.g. damping, averaging)

@dataclass
class TestRigConfig:
    """Complete test rig hardware configuration."""
    mock: bool
    mass: ScaleConfig
    flow: FlowControlConfig
    high_dp: DiffPressConfig
    low_dp: DiffPressConfig
    alicat_shared: AlicatSharedSerial = field(default_factory=AlicatSharedSerial)

    def __post_init__(self):
        """Basic consistency checks."""
        # Ensure at least one Alicat has access to a serial port
        alicats = [self.flow, self.high_dp, self.low_dp]
        
        has_serial = any([self.alicat_shared.serial is not None, 
                          all([a.serial is not None for a in alicats])])

        if not has_serial:
            raise ValueError(
                "No serial configuration found for any Alicat device. "
                "Define alicat_shared.serial or a per-device serial."
            )
