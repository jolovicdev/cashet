from __future__ import annotations

from pathlib import Path

import pytest

from cashet import Client


@pytest.fixture
def store_dir(tmp_path: Path) -> Path:
    d = tmp_path / ".cashet"
    d.mkdir()
    return d


@pytest.fixture
def client(store_dir: Path) -> Client:
    return Client(store_dir=store_dir)
