import pytest

from async_dag.execution_result import ExecutionResult
from async_dag.task_manager import TaskManager, build_dag


async def imm() -> int:
    return 0


async def test_invoke_with_task_manager_mismatch_errors() -> None:
    with pytest.raises(ValueError):
        with build_dag() as tm:
            node = tm.add_node(imm)

        other_tm = TaskManager[None]()
        execution_result = ExecutionResult(other_tm, None)

        await node.invoke(None, execution_result)


async def test_extract_result_with_task_manager_mismatch_errors() -> None:
    with pytest.raises(ValueError):
        with build_dag() as tm:
            node = tm.add_node(imm)

        other_tm = TaskManager[None]()
        execution_result = ExecutionResult(other_tm, None)

        node.extract_result(execution_result)


async def test_extract_result_before_sort_errors() -> None:
    with pytest.raises(ValueError):
        tm = TaskManager[None]()
        node = tm.add_node(imm)
        execution_result = ExecutionResult(tm, None)

        node.extract_result(execution_result)


async def test_invoke_before_sort_errors() -> None:
    with pytest.raises(ValueError):
        tm = TaskManager[None]()
        node = tm.add_node(imm)
        execution_result = ExecutionResult(tm, None)

        await node.invoke(None, execution_result)


async def test_extract_result_should_return_value_from_index_from_id() -> None:
    expected = 999
    with build_dag() as tm:
        node = tm.add_node(imm)
    execution_result = ExecutionResult(tm, None)
    execution_result._results[node._id] = expected

    assert expected == node.extract_result(execution_result)


async def test_invoke_returns_its_value() -> None:
    with build_dag() as tm:
        node = tm.add_node(imm)
    execution_result = ExecutionResult(tm, None)

    result = await node.invoke(None, execution_result)

    assert result == await imm()
