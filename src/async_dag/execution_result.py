from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .task_manager import TaskManager


class ExecutionResult[_ParametersType]:
    def __init__(
        self, results: list[object], task_manager: "TaskManager[_ParametersType]"
    ) -> None:
        self._results = results
        self._task_manager = task_manager
