import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import pytest

from async_dag.task_manager import TaskManager, build_dag


@dataclass
class Input:
    starting_number: int


async def imm() -> int:
    return 1


async def from_args(args: Input) -> int:
    return args.starting_number


async def inc(n: int) -> int:
    return n + 1


async def to_str(n: int) -> str:
    return str(n)


async def to_int(n: str, _: Input) -> int:
    return int(n)


async def inc_str(n: str) -> int:
    return int(n) + 1


async def inc_max(a: int, b: int) -> int:
    return max(a, b) + 1


async def test_task_manager_sanity() -> None:
    tm = TaskManager[Input]()
    starting_node = tm.add_node(from_args)

    inc_1 = tm.add_node(inc, starting_node)

    str_node = tm.add_node(to_str, starting_node)
    str_to_int_node = tm.add_node(to_int, str_node)
    int_node = tm.add_node(inc_str, str_node)
    inc_2 = tm.add_node(inc, int_node)

    end_1 = tm.add_node(inc_max, inc_2, inc_1)

    end_2 = tm.add_node(inc_max, inc_1, starting_node)

    end_3 = tm.add_node(inc_max, str_to_int_node, starting_node)

    tm.sort()

    result_1 = await tm.invoke(Input(0))

    result_2 = await tm.invoke(Input(999))

    assert end_1.extract_result(result_1) == 3
    assert end_2.extract_result(result_1) == 2
    assert end_3.extract_result(result_1) == 1

    assert end_1.extract_result(result_2) == 1002
    assert end_2.extract_result(result_2) == 1001
    assert end_3.extract_result(result_2) == 1000


async def test_build_dag_sanity() -> None:
    with build_dag(Input) as tm:
        starting_node = tm.add_node(from_args)

        inc_1 = tm.add_node(inc, starting_node)

        str_node = tm.add_node(to_str, starting_node)
        str_to_int_node = tm.add_node(to_int, str_node)
        int_node = tm.add_node(inc_str, str_node)
        inc_2 = tm.add_node(inc, int_node)

        end_1 = tm.add_node(inc_max, inc_2, inc_1)

        end_2 = tm.add_node(inc_max, inc_1, starting_node)

        end_3 = tm.add_node(inc_max, str_to_int_node, starting_node)

    result_1 = await tm.invoke(Input(0))

    result_2 = await tm.invoke(Input(999))

    assert end_1.extract_result(result_1) == 3
    assert end_2.extract_result(result_1) == 2
    assert end_3.extract_result(result_1) == 1

    assert end_1.extract_result(result_2) == 1002
    assert end_2.extract_result(result_2) == 1001
    assert end_3.extract_result(result_2) == 1000


async def test_multiple_invocation() -> None:
    with build_dag(Input) as tm:
        from_args_node = tm.add_node(from_args)

    expected_1 = 0
    result_1 = await tm.invoke(Input(expected_1))

    expected_2 = 999
    result_2 = await tm.invoke(Input(expected_2))

    assert from_args_node.extract_result(result_1) == expected_1
    assert from_args_node.extract_result(result_2) == expected_2


async def test_dag_with_single_node() -> None:
    with build_dag() as tm:
        starting_node = tm.add_node(imm)
    result = await tm.invoke(None)

    assert starting_node.extract_result(result) == await imm()


async def test_dag_with_zero_nodes() -> None:
    with build_dag() as tm:
        pass
    result = await tm.invoke(None)
    assert result is not None


async def test_sorting_twice_should_error() -> None:
    with pytest.raises(ValueError), build_dag() as tm:
        tm.sort()

    with pytest.raises(ValueError):
        tm = TaskManager[None]()
        tm.sort()
        tm.sort()


async def test_adding_nodes_after_sort_should_error() -> None:
    with pytest.raises(ValueError):
        with build_dag() as tm:
            pass
        tm.add_node(imm)

    with pytest.raises(ValueError):
        tm = TaskManager[None]()
        tm.sort()
        tm.add_node(imm)


async def test_invoke_before_sort_should_error() -> None:
    with pytest.raises(ValueError), build_dag() as tm:
        await tm.invoke(None)

    with pytest.raises(ValueError):
        tm = TaskManager[None]()
        await tm.invoke(None)


async def test_calling_order_of_dag() -> None:
    def define_step(
        expected_state: int, delay: float
    ) -> Callable[[None, Input], Awaitable[None]]:
        async def inner(_: None, global_state: Input) -> None:
            await asyncio.sleep(delay)
            assert expected_state == global_state.starting_number
            global_state.starting_number += 1
            return

        return inner

    def define_merge_step(
        expected_state: int, delay: float
    ) -> Callable[[None, None, Input], Awaitable[None]]:
        async def inner(_a: None, _b: None, global_state: Input) -> None:
            await asyncio.sleep(delay)
            assert expected_state == global_state.starting_number
            global_state.starting_number += 1
            return

        return inner

    with build_dag(Input) as tm:
        starting_node = tm.add_immediate_node(None)

        single_path_to_end_node_1 = tm.add_node(define_step(1, 0), starting_node)
        single_path_to_end_node_2 = tm.add_node(
            define_step(2, 0), single_path_to_end_node_1
        )

        merge_path_to_end_node_1_1 = tm.add_node(define_step(3, 0.1), starting_node)
        merge_path_to_end_node_1_2 = tm.add_node(define_step(4, 0.15), starting_node)

        merge_path_to_end_node_2 = tm.add_node(
            define_merge_step(5, 0),
            merge_path_to_end_node_1_1,
            merge_path_to_end_node_1_2,
        )

        tm.add_node(
            define_merge_step(6, 0),
            single_path_to_end_node_2,
            merge_path_to_end_node_2,
        )

    await tm.invoke(Input(1))


async def test_add_node_with_mixed_managers_errors() -> None:
    with pytest.raises(ValueError), build_dag() as tm_1, build_dag() as tm_2:
        starting_node = tm_1.add_node(imm)
        tm_2.add_node(inc, starting_node)


async def test_sort_with_non_dag_graph_errors() -> None:
    with pytest.raises(ValueError), build_dag() as tm:
        node_1 = tm.add_node(imm)
        node_2 = tm.add_node(imm)

        node_1._dependencies_ids = [node_2._id]
        node_2._dependencies_ids = [node_1._id]


async def test_immediate_node_should_return_its_value() -> None:
    expected = "Hello World"
    with build_dag() as tm:
        imm_node = tm.add_immediate_node(expected)

    result = await tm.invoke(None)

    assert imm_node.extract_result(result) == expected


async def test_duplicate_task_node_input() -> None:
    expected_value = 0

    async def merge(_a: None, _b: None, input_: Input) -> None:
        assert input_.starting_number == expected_value
        input_.starting_number += 1
        return

    with build_dag(Input) as tm:
        starting_node = tm.add_immediate_node(None)

        tm.add_node(merge, starting_node, starting_node)

    await tm.invoke(Input(expected_value))
