from __future__ import annotations

import os

DEFAULT_TEST_REDIS_URL = "redis://localhost:6379/15"


def redis_test_url() -> str:
    return os.environ.get("CASHET_TEST_REDIS_URL", DEFAULT_TEST_REDIS_URL)


class PickleableCustom:
    def __init__(self, val: int) -> None:
        self.val = val

    def __eq__(self, other: object) -> bool:
        return isinstance(other, PickleableCustom) and self.val == other.val
