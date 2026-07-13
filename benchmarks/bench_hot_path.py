from __future__ import annotations

import asyncio
import statistics
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from cashet import Client
from cashet.async_client import AsyncClient
from cashet.hashing import build_task_def

ROUNDS = 300


def transform(data: list[int], scale: float = 2.0) -> list[float]:
    return [x * scale for x in data]


def _timed(fn: Callable[[], Any], rounds: int = ROUNDS) -> tuple[float, float]:
    fn()
    samples: list[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1e6)
    return statistics.median(samples), min(samples)


def _report(label: str, median_us: float, min_us: float) -> None:
    print(f"{label:<28} median {median_us:9.1f} us   min {min_us:9.1f} us")


def bench_sync(root: Path) -> None:
    with Client(store_dir=root / "sync") as client:
        client.submit(transform, [1, 2, 3]).load()
        _report(
            "hash (build_task_def)",
            *_timed(lambda: build_task_def(transform, ([1, 2, 3],), {})),
        )
        _report("sync hit (submit)", *_timed(lambda: client.submit(transform, [1, 2, 3])))
        _report(
            "sync hit (submit + load)",
            *_timed(lambda: client.submit(transform, [1, 2, 3]).load()),
        )


def bench_async(root: Path) -> None:
    async def run() -> None:
        client = AsyncClient(store_dir=root / "async")
        ref = await client.submit(transform, [1, 2, 3])
        await ref.load()

        samples: list[float] = []
        for _ in range(ROUNDS):
            start = time.perf_counter()
            await client.submit(transform, [1, 2, 3])
            samples.append((time.perf_counter() - start) * 1e6)
        _report("async hit (submit)", statistics.median(samples), min(samples))
        await client.close()

    asyncio.run(run())


def bench_miss(root: Path) -> None:
    with Client(store_dir=root / "miss") as client:
        samples: list[float] = []
        for i in range(50):
            start = time.perf_counter()
            client.submit(transform, [i])
            samples.append((time.perf_counter() - start) * 1e6)
        _report("sync miss (run + store)", statistics.median(samples), min(samples))


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="cashet-bench-"))
    print(f"store: {root}  rounds: {ROUNDS}")
    bench_sync(root)
    bench_async(root)
    bench_miss(root)


if __name__ == "__main__":
    main()
