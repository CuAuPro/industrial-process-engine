from .base import ProcessModelController


class ContinuousModelController(ProcessModelController):
    """Material-flow model; tracking/transport are composed as shared subsystems."""

    name = "continuous"

    def status(self) -> dict[str, object]:
        return {
            "material_flow": True,
            "membership": "multiple",
            "close_run": "last_product_exit",
        }
