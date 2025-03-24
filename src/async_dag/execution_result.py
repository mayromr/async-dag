import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .task_manager import TaskManager
    from .task_node import TaskNode


class ExecutionResult[_ParameterType]:
    def __init__(
        self, task_manager: "TaskManager[_ParameterType]", parameter: _ParameterType
    ) -> None:
        self._tasks = task_manager._tasks
        self._results: list[object] = [None] * len(self._tasks)
        self._task_manager = task_manager
        self._tasks_missing_dependencies_count = [
            len(task._dependencies_ids) for task in self._tasks
        ]
        self._starting_nodes_id = self._task_manager._starting_nodes_id
        self._parameter = parameter

    async def _invoke_task(
        self, task: "TaskNode[_ParameterType, object]", tg: asyncio.TaskGroup
    ) -> None:
        self._on_task_completion(task, await task.invoke(self), tg)

    def _on_task_completion(
        self,
        task: "TaskNode[_ParameterType, object]",
        result: object,
        tg: asyncio.TaskGroup,
    ) -> None:
        self._results[task._id] = result

        for dependent_id in task._dependents_ids:
            self._tasks_missing_dependencies_count[dependent_id] -= 1
            if self._tasks_missing_dependencies_count[dependent_id] <= 0:
                tg.create_task(self._invoke_task(self._tasks[dependent_id], tg))

    async def _invoke(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for node_id in self._starting_nodes_id:
                task = self._tasks[node_id]
                if task is self._task_manager._parameter_node:
                    self._on_task_completion(task, self._parameter, tg)
                else:
                    tg.create_task(self._invoke_task(task, tg))
