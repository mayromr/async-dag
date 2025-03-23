async-dag
---
A simple library for running complex DAG of async tasks

#### example

```python
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable

from async_dag import build_dag


@dataclass
class Event:
    timestamp: datetime
    location: str


class DatabaseClient:
    async def insert(self, event: Event) -> bool:
        # simulate async access to the database
        await asyncio.sleep(0.5)

        return True


class HttpClient:
    async def fetch(self, url: str) -> Event:
        # simulate async http request
        await asyncio.sleep(0.5)

        return Event(timestamp=datetime.now(), location=url)

    async def publish_logs(self, results: list[bool]) -> None:
        # simulate async http request
        await asyncio.sleep(0.5)


@dataclass
class Parameters:
    http_client: HttpClient
    db_client: DatabaseClient
    allowed_locations: str


async def fetch_event(url: str, params: Parameters) -> Event:
    # NOTE: we have access to the invoke params, http client for example
    return await params.http_client.fetch(url)


async def insert_to_db(event: Event, params: Parameters) -> bool:
    if event.location != params.allowed_locations:
        return False

    return await params.db_client.insert(event)


async def publish_results(result_1: bool, result_2: bool, params: Parameters) -> None:
    await params.http_client.publish_logs([result_1, result_2])


# NOTE: we don't have to request receive the Parameters argument, we can also request nodes that are not in the last batch
async def logger(
    event_1: Event, result_1: bool, event_2: Event, result_2: bool
) -> None:
    print(event_1, result_1, event_2, result_2)


def url_immediate(url: str) -> Callable[[], Awaitable[str]]:
    async def _inner() -> str:
        return url

    return _inner


with build_dag(Parameters) as tm:
    moon_url = tm.add_node(url_immediate("moon"))
    moon_event = tm.add_node(fetch_event, moon_url)
    moon_insert = tm.add_node(insert_to_db, moon_event)

    sun_url = tm.add_node(url_immediate("sun"))
    sun_event = tm.add_node(fetch_event, sun_url)
    sun_insert = tm.add_node(insert_to_db, sun_event)

    tm.add_node(publish_results, moon_insert, sun_insert)

    tm.add_node(logger, moon_event, moon_insert, sun_event, sun_insert)


async def main():
    http_client = HttpClient()
    db_client = DatabaseClient()

    # prints due to logger
    # Event(timestamp=datetime.datetime(2025, 3, 23, 16, 13, 55, 498349), location='moon') True Event(timestamp=datetime.datetime(2025, 3, 23, 16, 13, 55, 498361), location='sun') False
    first_result = await tm.invoke(Parameters(http_client, db_client, "moon"))

    # Event(timestamp=datetime.datetime(2025, 3, 23, 16, 13, 55, 498349), location='moon')
    # NOTE: the result of each node using the ExecutionResult object
    print(moon_event.extract_result(first_result))
    # True
    print(moon_insert.extract_result(first_result))

    # prints due to logger
    # Event(timestamp=datetime.datetime(2025, 3, 23, 16, 13, 57, 48707), location='moon') False Event(timestamp=datetime.datetime(2025, 3, 23, 16, 13, 57, 48717), location='sun') True
    # NOTE: we can use the same TaskGroup many times, there is no need to rebuild the DAG
    second_result = await tm.invoke(Parameters(http_client, db_client, "sun"))

    # Event(timestamp=datetime.datetime(2025, 3, 23, 16, 13, 57, 48707), location='moon')
    print(moon_event.extract_result(second_result))  # prints:
    # False
    print(moon_insert.extract_result(second_result))  # prints: sun


if __name__ == "__main__":
    asyncio.run(main())
```
