from __future__ import annotations

from pathlib import Path

import pytest

from cashet import Client


def pytest_configure(config: pytest.Config) -> None:
    import os

    if os.environ.get("CASHET_REDIS"):
        config.redis_available = True
        return
    try:
        import redis
        r = redis.from_url("redis://localhost:6379/0")
        r.ping()
        r.close()
    except Exception:
        config.redis_available = False
    else:
        config.redis_available = True


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if getattr(config, "redis_available", False):
        return
    skip = pytest.mark.skip(reason="Redis not available — start container or set CASHET_REDIS=1")
    for item in items:
        if item.get_closest_marker("redis"):
            item.add_marker(skip)


@pytest.fixture
def store_dir(tmp_path: Path) -> Path:
    d = tmp_path / ".cashet"
    d.mkdir()
    return d


@pytest.fixture
def client(store_dir: Path) -> Client:
    return Client(store_dir=store_dir)
