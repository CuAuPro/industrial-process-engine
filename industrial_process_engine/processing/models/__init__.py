from __future__ import annotations

from industrial_process_engine.config import ProcessConfig

from .base import ProcessModelController
from .continuous import ContinuousModelController
from .cycle import CycleModelController
from .transformation import TransformationModelController


def create_model_controller(process: ProcessConfig) -> ProcessModelController:
    model = process.model
    if model == "cycle":
        assert process.membership is not None and process.close_run is not None
        return CycleModelController(process.membership, process.close_run)
    if model == "continuous":
        return ContinuousModelController()
    if model == "transformation":
        return TransformationModelController()
    raise ValueError(f"unsupported process model: {model}")


__all__ = [
    "ContinuousModelController", "CycleModelController",
    "ProcessModelController", "TransformationModelController", "create_model_controller",
]
