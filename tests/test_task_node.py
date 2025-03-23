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
        execution_result = ExecutionResult([], other_tm)

        await node.invoke(None, execution_result)


async def test_extract_result_with_task_manager_mismatch_errors() -> None:
    with pytest.raises(ValueError):
        with build_dag() as tm:
            node = tm.add_node(imm)

        other_tm = TaskManager[None]()
        execution_result = ExecutionResult([], other_tm)

        node.extract_result(execution_result)


async def test_extract_result_before_sort_errors() -> None:
    with pytest.raises(ValueError):
        tm = TaskManager[None]()
        node = tm.add_node(imm)
        execution_result = ExecutionResult([], tm)

        node.extract_result(execution_result)


async def test_invoke_before_sort_errors() -> None:
    with pytest.raises(ValueError):
        tm = TaskManager[None]()
        node = tm.add_node(imm)
        execution_result = ExecutionResult([], tm)

        await node.invoke(None, execution_result)


async def test_extract_result_should_return_value_from_index_from_id() -> None:
    expected = 999
    with build_dag() as tm:
        node = tm.add_node(imm)
    execution_result = ExecutionResult([999], tm)

    assert expected == node.extract_result(execution_result)


async def test_invoke_assign_results_to_index_from_id() -> None:
    with build_dag() as tm:
        node = tm.add_node(imm)
    execution_result = ExecutionResult([None], tm)

    await node.invoke(None, execution_result)

    assert execution_result._results[0] == await imm()
    assert node.extract_result(execution_result) == await imm()
