from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from typing import Never, overload

from .execution_result import ExecutionResult
from .state import State
from .task_node import TaskNode

type TaskCallback[_ReturnType, *_Inputs] = Callable[[*_Inputs], Awaitable[_ReturnType]]
type TaskNodeOrImmediate[_ParameterType, _ReturnType] = (
    TaskNode[_ParameterType, _ReturnType] | _ReturnType
)


async def _unreachable(*_: object) -> Never:
    raise ValueError("unreachable")


class TaskManager[_ParameterType]:
    """
    ### TaskManager
    `TaskManager` is the main building block of `async_dag`, it provides an interface to build and run DAGs.
    The four main APIs that are relevant to you are:
    1. `add_node(async_task, param_a, param_b, ...)` - adds a new node to the graph go to function signature for feature information.
    2. `parameter_node` - a spacial property of `TaskManager` that when passed as a dependency of a task will resolve to the parameter passed to `invoke`.
    3. `sort()` - sorts the DAG and ready the TaskManager up for upcoming `invoke` calls.
    4. `invoke(parameter)` - execute the tasks in the DAG with a given parameter returns an `ExecutionResult`.
    4. `TaskNode.extract_result(execution_result)` - extracts the result returned from the task passed to the node.

    You can also use the helper function `build_dag` that provides a context manager and handle calling sort for you.

    All of the listed methods, properties and functions listed here have strings with a deeper explanation.

    #### Example
    ```python
    async def add(n: int) -> int:
        return n + 1

    tm = TaskManager[int]() # Define a dag that receives an int as a parameter

    # Build the DAG
    node_1 = tm.add_node(add, tm.parameter_node) # use the parameter passed to `tm.invoke`
    node_2 = tm.add_node(add, node_1) # use the value returned from node_1
    node_3 = tm.add_node(add, node_2) # use the value returned from node_2

    tm.sort() # sorts the DAG and ready the `TaskManager` for `invoke` calls

    execution_result = await tm.invoke(0) # Invoke our DAG

    # Extract the result from one of the nodes
    print(node_3.extract_result(execution_result)) # prints 3
    ```
    """

    def __init__(self) -> None:
        # NOTE: the tasks in _tasks must be a contiguous array of sorted by _id
        self._tasks: list[TaskNode[_ParameterType, object]] = []
        self._max_depth = 0
        self._starting_nodes_id: list[int] = []
        self._is_sorted: bool = False
        self._parameter_node: TaskNode[_ParameterType, _ParameterType] | None = None

    async def invoke(
        self, parameter: _ParameterType
    ) -> ExecutionResult[_ParameterType]:
        """
        Execute the DAG, this functions `parameter` argument will be passed to each node that is depending on the spacial `parameter_node`.

        This function should only be called after `sort` was called, any calls to it before `sort` was called raise a `ValueError`.

        If any task raises an exception this function will raise a `ExceptionGroup` with that exception and any other exceptions raised during the cancellation of the rest of the tasks.
        """
        if not self._is_sorted:
            raise ValueError("'invoke' can not be called before 'sort'")

        execution_result = ExecutionResult(self, parameter)
        await execution_result._invoke()
        return execution_result

    def sort(self) -> None:
        """
        Ready the `TaskManager` for `invoke` calls, this method check that the created graph is indeed a DAG,
        If a cycle is detected a `ValueError` will be raised.

        This function should only be called once, any call after the first will raise a `ValueError`.

        Do not call this function if you are using the `build_dag` helper function.
        """
        if self._is_sorted:
            raise ValueError("'sort' can only be called once")

        def visit(node: TaskNode[_ParameterType, object]) -> None:
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

    @property
    def parameter_node(self) -> TaskNode[_ParameterType, _ParameterType]:
        """A spacial node that represents the parameter value, if a node `TaskNode` depends on this node it will receive the value passed to `invoke`."""
        if self._parameter_node is None:
            self._parameter_node = self._add_node(_unreachable)
        return self._parameter_node

    def add_immediate_node[_ReturnType](
        self, value: _ReturnType
    ) -> TaskNode[_ParameterType, _ReturnType]:
        async def get_value() -> _ReturnType:
            return value

        return self._add_node(get_value)

    @overload
    def add_node[_ReturnType](
        self,
        task: TaskCallback[_ReturnType],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1](
        self,
        task: TaskCallback[_ReturnType, _I_1],
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2](
        self,
        task: TaskCallback[_ReturnType, _I_1, _I_2],
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3](
        self,
        task: TaskCallback[_ReturnType, _I_1, _I_2, _I_3],
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4](
        self,
        task: TaskCallback[_ReturnType, _I_1, _I_2, _I_3, _I_4],
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5](
        self,
        task: TaskCallback[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5],
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6](
        self,
        task: TaskCallback[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6],
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6, _I_7](
        self,
        task: TaskCallback[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6, _I_7],
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6, _I_7, _I_8](
        self,
        task: TaskCallback[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6, _I_7, _I_8],
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

    @overload
    def add_node[_ReturnType, _I_1, _I_2, _I_3, _I_4, _I_5, _I_6, _I_7, _I_8, _I_9](
        self,
        task: TaskCallback[
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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
        arg_11: TaskNodeOrImmediate[_ParameterType, _I_11],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
        arg_11: TaskNodeOrImmediate[_ParameterType, _I_11],
        arg_12: TaskNodeOrImmediate[_ParameterType, _I_12],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
        arg_11: TaskNodeOrImmediate[_ParameterType, _I_11],
        arg_12: TaskNodeOrImmediate[_ParameterType, _I_12],
        arg_13: TaskNodeOrImmediate[_ParameterType, _I_13],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
        arg_11: TaskNodeOrImmediate[_ParameterType, _I_11],
        arg_12: TaskNodeOrImmediate[_ParameterType, _I_12],
        arg_13: TaskNodeOrImmediate[_ParameterType, _I_13],
        arg_14: TaskNodeOrImmediate[_ParameterType, _I_14],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
        arg_11: TaskNodeOrImmediate[_ParameterType, _I_11],
        arg_12: TaskNodeOrImmediate[_ParameterType, _I_12],
        arg_13: TaskNodeOrImmediate[_ParameterType, _I_13],
        arg_14: TaskNodeOrImmediate[_ParameterType, _I_14],
        arg_15: TaskNodeOrImmediate[_ParameterType, _I_15],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
        arg_11: TaskNodeOrImmediate[_ParameterType, _I_11],
        arg_12: TaskNodeOrImmediate[_ParameterType, _I_12],
        arg_13: TaskNodeOrImmediate[_ParameterType, _I_13],
        arg_14: TaskNodeOrImmediate[_ParameterType, _I_14],
        arg_15: TaskNodeOrImmediate[_ParameterType, _I_15],
        arg_16: TaskNodeOrImmediate[_ParameterType, _I_16],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
        arg_11: TaskNodeOrImmediate[_ParameterType, _I_11],
        arg_12: TaskNodeOrImmediate[_ParameterType, _I_12],
        arg_13: TaskNodeOrImmediate[_ParameterType, _I_13],
        arg_14: TaskNodeOrImmediate[_ParameterType, _I_14],
        arg_15: TaskNodeOrImmediate[_ParameterType, _I_15],
        arg_16: TaskNodeOrImmediate[_ParameterType, _I_16],
        arg_17: TaskNodeOrImmediate[_ParameterType, _I_17],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
        arg_11: TaskNodeOrImmediate[_ParameterType, _I_11],
        arg_12: TaskNodeOrImmediate[_ParameterType, _I_12],
        arg_13: TaskNodeOrImmediate[_ParameterType, _I_13],
        arg_14: TaskNodeOrImmediate[_ParameterType, _I_14],
        arg_15: TaskNodeOrImmediate[_ParameterType, _I_15],
        arg_16: TaskNodeOrImmediate[_ParameterType, _I_16],
        arg_17: TaskNodeOrImmediate[_ParameterType, _I_17],
        arg_18: TaskNodeOrImmediate[_ParameterType, _I_18],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
        arg_11: TaskNodeOrImmediate[_ParameterType, _I_11],
        arg_12: TaskNodeOrImmediate[_ParameterType, _I_12],
        arg_13: TaskNodeOrImmediate[_ParameterType, _I_13],
        arg_14: TaskNodeOrImmediate[_ParameterType, _I_14],
        arg_15: TaskNodeOrImmediate[_ParameterType, _I_15],
        arg_16: TaskNodeOrImmediate[_ParameterType, _I_16],
        arg_17: TaskNodeOrImmediate[_ParameterType, _I_17],
        arg_18: TaskNodeOrImmediate[_ParameterType, _I_18],
        arg_19: TaskNodeOrImmediate[_ParameterType, _I_19],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

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
        arg_1: TaskNodeOrImmediate[_ParameterType, _I_1],
        arg_2: TaskNodeOrImmediate[_ParameterType, _I_2],
        arg_3: TaskNodeOrImmediate[_ParameterType, _I_3],
        arg_4: TaskNodeOrImmediate[_ParameterType, _I_4],
        arg_5: TaskNodeOrImmediate[_ParameterType, _I_5],
        arg_6: TaskNodeOrImmediate[_ParameterType, _I_6],
        arg_7: TaskNodeOrImmediate[_ParameterType, _I_7],
        arg_8: TaskNodeOrImmediate[_ParameterType, _I_8],
        arg_9: TaskNodeOrImmediate[_ParameterType, _I_9],
        arg_10: TaskNodeOrImmediate[_ParameterType, _I_10],
        arg_11: TaskNodeOrImmediate[_ParameterType, _I_11],
        arg_12: TaskNodeOrImmediate[_ParameterType, _I_12],
        arg_13: TaskNodeOrImmediate[_ParameterType, _I_13],
        arg_14: TaskNodeOrImmediate[_ParameterType, _I_14],
        arg_15: TaskNodeOrImmediate[_ParameterType, _I_15],
        arg_16: TaskNodeOrImmediate[_ParameterType, _I_16],
        arg_17: TaskNodeOrImmediate[_ParameterType, _I_17],
        arg_18: TaskNodeOrImmediate[_ParameterType, _I_18],
        arg_19: TaskNodeOrImmediate[_ParameterType, _I_19],
        arg_20: TaskNodeOrImmediate[_ParameterType, _I_20],
    ) -> TaskNode[_ParameterType, _ReturnType]: ...

    # TODO: remove all the @overload functions once https://github.com/python/typing/issues/1216 get solved
    def add_node[_ReturnType, *_InputsType](  # type: ignore
        self,
        task: Callable[[*_InputsType], Awaitable[_ReturnType]],
        *dependencies: TaskNodeOrImmediate[_ParameterType, object],
    ) -> TaskNode[_ParameterType, _ReturnType]:
        """
        This functions is the heart of this library, each call to this function adds a new node in our execution DAG.

        This function receives a `task` which is a partially applied coroutine (uncalled) which will get called once all the `dependencies` passed in the following arguments are satisfied.

        The `dependencies` parameters could either be an immediate value (not a `TaskNode`) that will get resolved immediately upon calling `invoke` (hance the name immediate),
        or a `TaskNode` which will act as a dependency to that task.
        You can look at this function the same way as you look at `functools.partial`, but in addition to parameters we can also pass the task dependencies.
        """
        return self._add_node(
            task,
            *(
                dep if isinstance(dep, TaskNode) else self.add_immediate_node(dep)
                for dep in dependencies
            ),
        )

    def _add_node[_ReturnType, *_InputsType](
        self,
        task: Callable[[*_InputsType], Awaitable[_ReturnType]],
        *dependencies: TaskNode[_ParameterType, object],
    ) -> TaskNode[_ParameterType, _ReturnType]:
        if self._is_sorted:
            raise ValueError("'add_node' can not be called after 'sort'")

        for dep in dependencies:
            if dep._task_manager is not self:
                raise ValueError(
                    f"Task manager mismatch, expected: {self} but got: {dep._task_manager}"
                )

        task_node = TaskNode(
            task,
            self,
            [dep_id._id for dep_id in dependencies],
            len(self._tasks),
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
    """
    A helper function that returns a context manager that calls sort for you on the `TaskManager` it creates.
    This is useful for creating an indented section that defines your DAG.

    The first parameter defines the type T of `TaskManager[T]` which sets the `invoke(parameter: T)` parameter type.

    #### Example:
    ```python
    async def add(n: int) -> int:
        return n + 1

    # Define a dag that receives an int as a parameter
    with build_dag(int) as tm:
        # Build the DAG
        node_1 = tm.add_node(add, tm.parameter_node) # use the parameter passed to `tm.invoke`
        node_2 = tm.add_node(add, node_1) # use the value returned from node_1
        node_3 = tm.add_node(add, node_2) # use the value returned from node_2

    # After we exited the `with` block we can already call `tm.invoke` because the context manager handled the `sort` call for us.

    execution_result = await tm.invoke(0) # Invoke our DAG

    # Extract the result from one of the nodes
    print(node_3.extract_result(execution_result)) # prints 3
    ```
    """
    task_manager = TaskManager[T]()

    yield task_manager

    task_manager.sort()
