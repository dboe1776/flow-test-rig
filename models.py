import json
import datetime as dt
import time
from dataclasses import dataclass, field, asdict
from typing import Optional, Literal, List, Union, Any
from periphs import alicat, scale
from enum import StrEnum, Enum, auto
from loguru import logger
from pathlib import Path

@dataclass(kw_only=True)
class TestRigDF:
    time: float = field(
        default_factory=lambda:time.time()
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
        timestamp = dt.datetime.fromtimestamp(self.time).isoformat(timespec='seconds')
        return {'time':timestamp,**mass,**flow,**low_dp,**high_dp}

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

SINK_TYPE_JSON_FOLDER = "json_folder"
SINK_TYPE_INFLUXDB    = "influxdb"

@dataclass(kw_only=True)
class BaseDataSink:
    """Fields common to all data sinks"""
    enabled: bool = True
    sample_period: int = 60           # seconds
    name: str = ""                    # optional friendly name for logs/UI

@dataclass(kw_only=True)
class JsonFolderSink(BaseDataSink):
    type: Literal["json_folder"] = 'json_folder'
    folder: Path = field(default_factory=lambda: Path("st-data", "records"))

@dataclass(kw_only=True)
class CsvSink(BaseDataSink):
    type: Literal["csv_file"] = "csv_file"
    folder: Path = field(default_factory=lambda: Path("st-data", "records"))
    # Optional: delimiter = ",", quotechar = '"', etc. if needed later

@dataclass(kw_only=True)
class InfluxSink(BaseDataSink):
    type: Literal["influxdb"] = 'influxdb'
    url: str
    user: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    org: Optional[str] = None
    database: str
    measurement: str = "rig_metrics"

# Union for type narrowing & config validation
AnyDataSink = Union[JsonFolderSink, InfluxSink, CsvSink]
DataSinkMap = {c.type:c for c in  BaseDataSink.__subclasses__()}

@dataclass
class TestRigConfig:
    """Complete test rig hardware configuration."""
    mock: bool
    mass: ScaleConfig
    flow: FlowControlConfig
    high_dp: DiffPressConfig
    low_dp: DiffPressConfig
    alicat_shared: AlicatSharedSerial = field(default_factory=AlicatSharedSerial)
    data_sinks: list[Any] = field(default_factory=dict)

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

        if not self.data_sinks:
            self.data_sinks = [JsonFolderSink(name='json_default')]
        else:
            sink_cls = [DataSinkMap.get(sink.get('type')) for sink in self.data_sinks]
            self.data_sinks = [ sink(**d) for d,sink in zip(self.data_sinks,sink_cls) if sink_cls is not None]

class EventNames(StrEnum):
    CHANGE_SETPOINT = 'change_setpoint'
    STOP_BUTTON = 'stop_button'
    NULL_EVENT = 'null_event'
    STATE_CHANGE = 'state_change'

class States(StrEnum):
    IDLE = 'idle'
    ACTIVE = 'active'
    FAULT = 'fault'

@dataclass(kw_only=True)
class Event:
    name: EventNames
    timestamp:int = field(default_factory=lambda:int(time.time()))
    retry:bool = False

    def model_dump_json(self):
        return json.dumps(asdict(self))

    @classmethod
    def model_load_json(cls,data):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            logger.warning('Invalid json passed to event parser')
            return None
        
        if cl := [c for c in Event.__subclasses__() if c.name == data.get('name')]:
            return cl[0](**data)
        
@dataclass(kw_only=True)
class StopButtonEvent(Event):
    name: EventNames = EventNames.STOP_BUTTON
    retry:bool = True

@dataclass(kw_only=True)
class SetpointEvent(Event):
    name: EventNames = EventNames.CHANGE_SETPOINT
    value: float

@dataclass(kw_only=True)
class NullEvent(Event):
    name: EventNames = EventNames.NULL_EVENT        

@dataclass(kw_only=True)
class StateChangeEvent(Event):
    new_state: States
    name: EventNames = EventNames.STATE_CHANGE