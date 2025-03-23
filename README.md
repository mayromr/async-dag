async-dag
---
A simple library for running complex DAG of async tasks

#### example

```python
import asyncio
from dataclasses import dataclass

from async_dag import TaskManager


@dataclass
class Input:
    starting_number: int


async def imm() -> int:
    return 1


async def inc(n: int) -> int:
    return n + 1


async def to_str(n: int) -> str:
    return str(n)


async def to_int(n: str, _input: Input) -> int:
    return int(n)


async def inc_str(n: str) -> int:
    return int(n) + 1


async def inc_max(a: int, b: int) -> int:
    return max(a, b) + 1


with build_dag(Input) as tm:
    starting_node = tm.add_node(imm)

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

```
