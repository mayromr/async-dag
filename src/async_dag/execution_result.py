import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .task_manager import TaskManager


class ExecutionResult[_ParametersType]:
    def __init__(
        self, task_manager: "TaskManager[_ParametersType]", parameters: _ParametersType
    ) -> None:
        self._tasks = task_manager._tasks
        self._results: list[object] = [None] * len(self._tasks)
        self._task_manager = task_manager
        self._tasks_missing_dependencies_count = [
            len(task._dependencies_ids) for task in self._tasks
        ]
        self._starting_nodes_id = self._task_manager._starting_nodes_id
        self._parameters = parameters

    async def _invoke_task(self, task_id: int, tg: asyncio.TaskGroup) -> None:
        task = self._tasks[task_id]

        result = await task.invoke(self._parameters, self)
        self._results[task._id] = result

        for dependent_id in task._dependents_ids:
            self._tasks_missing_dependencies_count[dependent_id] -= 1
            if self._tasks_missing_dependencies_count[dependent_id] <= 0:
                tg.create_task(self._invoke_task(dependent_id, tg))

    async def _invoke(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for node_id in self._starting_nodes_id:
                tg.create_task(self._invoke_task(node_id, tg))
