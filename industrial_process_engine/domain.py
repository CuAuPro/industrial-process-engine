from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class ServiceState(StrEnum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    ERROR = "ERROR"


class ProductState(StrEnum):
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    COMPLETE = "COMPLETE"
    ABORTED = "ABORTED"
    ERROR = "ERROR"


class SyncState(StrEnum):
    PENDING = "PENDING"
    SYNCED = "SYNCED"
    FAILED = "FAILED"


class TrackingStatus(StrEnum):
    OK = "OK"
    STOPPED = "STOPPED"
    LOST = "LOST"


class WindowQuality(StrEnum):
    GOOD = "GOOD"
    PARTIAL = "PARTIAL"
    DATA_GAP = "DATA_GAP"
    ESTIMATED_POSITION = "ESTIMATED_POSITION"
    TRACKING_LOST = "TRACKING_LOST"


class EventType(StrEnum):
    PRODUCT_ENTER = "PRODUCT_ENTER"
    PRODUCT_UPDATE = "PRODUCT_UPDATE"
    PRODUCT_EXIT = "PRODUCT_EXIT"
    PRODUCT_ABORT = "PRODUCT_ABORT"
    PROCESS_START = "PROCESS_START"
    PROCESS_END = "PROCESS_END"
    PROCESS_ABORT = "PROCESS_ABORT"
    LINE_START = "LINE_START"
    LINE_STOP = "LINE_STOP"


class CommandType(StrEnum):
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    STOP = "STOP"
    RESTART = "RESTART"


@dataclass(frozen=True, slots=True)
class SignalUpdate:
    name: str
    value: Any
    quality: bool
    timestamp_ms: int
    source_id: str
    topic: str
    source_timestamp_ms: int | None = None
    server_timestamp_ms: int | None = None


@dataclass(frozen=True, slots=True)
class SignalBatch:
    updates: tuple[SignalUpdate, ...]
    timestamp_ms: int
    source: str
    reason: str = "update"
    errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class OpcUaReadRequest:
    names: tuple[str, ...]
    reason: str
    timestamp_ms: int


@dataclass(frozen=True, slots=True)
class LifecycleSnapshot:
    event: ProcessEvent
    signals: SignalBatch


@dataclass(frozen=True, slots=True)
class ProcessEvent:
    event_type: EventType
    timestamp_ms: int
    product_id: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    source: str = "explicit"
    parent_product_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ControlCommand:
    command: CommandType


@dataclass(frozen=True, slots=True)
class TimeTick:
    timestamp_ms: int


@dataclass(slots=True)
class SignalValue:
    value: Any
    quality: bool
    timestamp_ms: int


@dataclass(slots=True)
class ProductFieldValue:
    value: Any
    quality: bool
    timestamp_ms: int


@dataclass(slots=True)
class ProductContext:
    run_id: str
    product_id: str
    start_ts: int
    context: dict[str, ProductFieldValue] = field(default_factory=dict)

    def field(self, name: str) -> Any | None:
        value = self.context.get(name)
        return value.value if value is not None and value.quality else None


@dataclass(frozen=True, slots=True)
class ProcessProduct:
    product_id: str
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ProcessContext:
    run_id: str
    start_ts: int
    products: tuple[ProductContext, ...]


@dataclass(slots=True)
class WindowRecord:
    process_id: str
    run_id: str
    product_id: str
    segment_no: int
    window_no: int
    axis: str
    ts_start: int
    ts_end: int
    elapsed_start_s: float
    elapsed_end_s: float
    position_start_m: float | None
    position_end_m: float | None
    values: dict[str, Any | None]
    quality: WindowQuality


@dataclass(slots=True)
class ProcessTimeRecord:
    process_id: str
    ts_start: int
    ts_end: int
    values: dict[str, Any | None]
    quality: WindowQuality
