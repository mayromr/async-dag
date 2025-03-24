import inspect
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from typing import overload

from .execution_result import ExecutionResult
from .state import State
from .task_node import TaskNode

type TaskCallback[_ParametersType, _ReturnType, *_Inputs] = (
    Callable[[*_Inputs, _ParametersType], Awaitable[_ReturnType]]
    | Callable[[*_Inputs], Awaitable[_ReturnType]]
)


class TaskManager[_ParametersType]:
    def __init__(self) -> None:
        # NOTE: the tasks in _tasks must be a contiguous array of sorted by _id
        self._tasks: list[TaskNode[_ParametersType, object]] = []
        self._max_depth = 0
        self._starting_nodes_id: list[int] = []
        self._is_sorted: bool = False

    async def invoke(
        self, parameters: _ParametersType
    ) -> ExecutionResult[_ParametersType]:
        if not self._is_sorted:
            raise ValueError("'invoke' can not be called before 'sort'")

        execution_result = ExecutionResult(self, parameters)
        await execution_result._invoke()
        return execution_result

    def sort(self) -> None:
        if self._is_sorted:
            raise ValueError("'sort' can only be called once")

        def visit(node: TaskNode[_ParametersType, object]) -> None:
            if node._state == State.PERMANENT:
                return
            if node._state == State.TEMPORARY:
                raise ValueError("Cycle detected, graph is not a DAG")

            node._state = State.TEMPORARY
            for dep_task in [self._tasks[dep_id] for dep_id in node._dependencies_ids]:
                if node._depth <= dep_task._depth:
                    node._depth = dep_task._depth + 1
                    self._max_depth = max(node._depth, self._max_depth)
                visit(dep_task)

            node._state = State.PERMANENT

        for task in self._tasks:
            if task._state == State.UNDISCOVERED:
                visit(task)

        for task in self._tasks:
            if len(task._dependencies_ids) == 0:
                self._starting_nodes_id.append(task._id)
            for dep in task._dependencies_ids:
                self._tasks[dep]._dependents_ids.add(task._id)

        self._is_sorted = True

    def add_immediate_node[_ReturnType](
        self, value: _ReturnType
    ) -> TaskNode[_ParametersType, _ReturnType]:
        async def get_value() -> _ReturnType:
            return value

        return self.add_node(get_value)

    @overload
    def add_node[_ReturnType](
        self,
        task: TaskCallback[_ParametersType, _ReturnType],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1](
        self,
        task: TaskCallback[_ParametersType, _ReturnType, _I_1],
        arg_1: TaskNode[_ParametersType, _I_1],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2](
        self,
        task: TaskCallback[_ParametersType, _ReturnType, _I_1, _I_2],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3](
        self,
        task: TaskCallback[_ParametersType, _ReturnType, _I_1, _I_2, _I_3],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4](
        self,
        task: TaskCallback[_ParametersType, _ReturnType, _I_1, _I_2, _I_3, _I_4],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5](
        self,
        task: TaskCallback[_ParametersType, _ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6](
        self,
        task: TaskCallback[
            _ParametersType, _ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6, _I_7](
        self,
        task: TaskCallback[
            _ParametersType, _ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6, _I_7
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6, _I_7, _I_8](
        self,
        task: TaskCallback[
            _ParametersType, _ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6, _I_7, _I_8
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6, _I_7, _I_8, _I_9](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
        _I_11,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
            _I_11,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
        arg_11: TaskNode[_ParametersType, _I_11],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
        _I_11,
        _I_12,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
            _I_11,
            _I_12,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
        arg_11: TaskNode[_ParametersType, _I_11],
        arg_12: TaskNode[_ParametersType, _I_12],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
        _I_11,
        _I_12,
        _I_13,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
            _I_11,
            _I_12,
            _I_13,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
        arg_11: TaskNode[_ParametersType, _I_11],
        arg_12: TaskNode[_ParametersType, _I_12],
        arg_13: TaskNode[_ParametersType, _I_13],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
        _I_11,
        _I_12,
        _I_13,
        _I_14,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
            _I_11,
            _I_12,
            _I_13,
            _I_14,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
        arg_11: TaskNode[_ParametersType, _I_11],
        arg_12: TaskNode[_ParametersType, _I_12],
        arg_13: TaskNode[_ParametersType, _I_13],
        arg_14: TaskNode[_ParametersType, _I_14],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
        _I_11,
        _I_12,
        _I_13,
        _I_14,
        _I_15,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
            _I_11,
            _I_12,
            _I_13,
            _I_14,
            _I_15,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
        arg_11: TaskNode[_ParametersType, _I_11],
        arg_12: TaskNode[_ParametersType, _I_12],
        arg_13: TaskNode[_ParametersType, _I_13],
        arg_14: TaskNode[_ParametersType, _I_14],
        arg_15: TaskNode[_ParametersType, _I_15],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
        _I_11,
        _I_12,
        _I_13,
        _I_14,
        _I_15,
        _I_16,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
            _I_11,
            _I_12,
            _I_13,
            _I_14,
            _I_15,
            _I_16,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
        arg_11: TaskNode[_ParametersType, _I_11],
        arg_12: TaskNode[_ParametersType, _I_12],
        arg_13: TaskNode[_ParametersType, _I_13],
        arg_14: TaskNode[_ParametersType, _I_14],
        arg_15: TaskNode[_ParametersType, _I_15],
        arg_16: TaskNode[_ParametersType, _I_16],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
        _I_11,
        _I_12,
        _I_13,
        _I_14,
        _I_15,
        _I_16,
        _I_17,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
            _I_11,
            _I_12,
            _I_13,
            _I_14,
            _I_15,
            _I_16,
            _I_17,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
        arg_11: TaskNode[_ParametersType, _I_11],
        arg_12: TaskNode[_ParametersType, _I_12],
        arg_13: TaskNode[_ParametersType, _I_13],
        arg_14: TaskNode[_ParametersType, _I_14],
        arg_15: TaskNode[_ParametersType, _I_15],
        arg_16: TaskNode[_ParametersType, _I_16],
        arg_17: TaskNode[_ParametersType, _I_17],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
        _I_11,
        _I_12,
        _I_13,
        _I_14,
        _I_15,
        _I_16,
        _I_17,
        _I_18,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
            _I_11,
            _I_12,
            _I_13,
            _I_14,
            _I_15,
            _I_16,
            _I_17,
            _I_18,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
        arg_11: TaskNode[_ParametersType, _I_11],
        arg_12: TaskNode[_ParametersType, _I_12],
        arg_13: TaskNode[_ParametersType, _I_13],
        arg_14: TaskNode[_ParametersType, _I_14],
        arg_15: TaskNode[_ParametersType, _I_15],
        arg_16: TaskNode[_ParametersType, _I_16],
        arg_17: TaskNode[_ParametersType, _I_17],
        arg_18: TaskNode[_ParametersType, _I_18],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
        _I_11,
        _I_12,
        _I_13,
        _I_14,
        _I_15,
        _I_16,
        _I_17,
        _I_18,
        _I_19,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
            _I_11,
            _I_12,
            _I_13,
            _I_14,
            _I_15,
            _I_16,
            _I_17,
            _I_18,
            _I_19,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
        arg_11: TaskNode[_ParametersType, _I_11],
        arg_12: TaskNode[_ParametersType, _I_12],
        arg_13: TaskNode[_ParametersType, _I_13],
        arg_14: TaskNode[_ParametersType, _I_14],
        arg_15: TaskNode[_ParametersType, _I_15],
        arg_16: TaskNode[_ParametersType, _I_16],
        arg_17: TaskNode[_ParametersType, _I_17],
        arg_18: TaskNode[_ParametersType, _I_18],
        arg_19: TaskNode[_ParametersType, _I_19],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    @overload
    def add_node[
        _ReturnType,
        _I_1,
        _I_2,
        _I_3,
        _I_4,
        _I_5,
        _I_6,
        _I_7,
        _I_8,
        _I_9,
        _I_10,
        _I_11,
        _I_12,
        _I_13,
        _I_14,
        _I_15,
        _I_16,
        _I_17,
        _I_18,
        _I_19,
        _I_20,
    ](
        self,
        task: TaskCallback[
            _ParametersType,
            _ReturnType,
            _I_1,
            _I_2,
            _I_3,
            _I_4,
            _I_5,
            _I_6,
            _I_7,
            _I_8,
            _I_9,
            _I_10,
            _I_11,
            _I_12,
            _I_13,
            _I_14,
            _I_15,
            _I_16,
            _I_17,
            _I_18,
            _I_19,
            _I_20,
        ],
        arg_1: TaskNode[_ParametersType, _I_1],
        arg_2: TaskNode[_ParametersType, _I_2],
        arg_3: TaskNode[_ParametersType, _I_3],
        arg_4: TaskNode[_ParametersType, _I_4],
        arg_5: TaskNode[_ParametersType, _I_5],
        arg_6: TaskNode[_ParametersType, _I_6],
        arg_7: TaskNode[_ParametersType, _I_7],
        arg_8: TaskNode[_ParametersType, _I_8],
        arg_9: TaskNode[_ParametersType, _I_9],
        arg_10: TaskNode[_ParametersType, _I_10],
        arg_11: TaskNode[_ParametersType, _I_11],
        arg_12: TaskNode[_ParametersType, _I_12],
        arg_13: TaskNode[_ParametersType, _I_13],
        arg_14: TaskNode[_ParametersType, _I_14],
        arg_15: TaskNode[_ParametersType, _I_15],
        arg_16: TaskNode[_ParametersType, _I_16],
        arg_17: TaskNode[_ParametersType, _I_17],
        arg_18: TaskNode[_ParametersType, _I_18],
        arg_19: TaskNode[_ParametersType, _I_19],
        arg_20: TaskNode[_ParametersType, _I_20],
    ) -> TaskNode[_ParametersType, _ReturnType]: ...

    # TODO: remove all the @overload functions once https://github.com/python/typing/issues/1216 get solved
    def add_node[_ReturnType, *_InputsType](  # type: ignore
        self,
        task: Callable[[*_InputsType, _ParametersType], Awaitable[_ReturnType]]
        | Callable[[*_InputsType], Awaitable[_ReturnType]],
        *dependencies: TaskNode[_ParametersType, object],
    ) -> TaskNode[_ParametersType, _ReturnType]:
        if self._is_sorted:
            raise ValueError("'add_node' can not be called after 'sort'")

        for dep in dependencies:
            if dep._task_manager is not self:
                raise ValueError(
                    f"Task manager mismatch, expected: {self} but got: {dep._task_manager}"
                )

        task_signature = inspect.signature(task)

        task_node = TaskNode(
            task,
            self,
            [dep_id._id for dep_id in dependencies],
            len(self._tasks),
            len(task_signature.parameters) != len(dependencies),
        )
        self._tasks.append(task_node)
        return task_node


@overload
@contextmanager
def build_dag() -> Iterator[TaskManager[None]]: ...


@overload
@contextmanager
def build_dag[T](parameter_type: type[T]) -> Iterator[TaskManager[T]]: ...


@contextmanager  # type: ignore
def build_dag[T](_: type[T] | None = None) -> Iterator[TaskManager[T]]:
    task_manager = TaskManager[T]()

    yield task_manager

    task_manager.sort()
