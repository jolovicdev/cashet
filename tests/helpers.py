from __future__ import annotations


class PickleableCustom:
    def __init__(self, val: int) -> None:
        self.val = val

    def __eq__(self, other: object) -> bool:
        return isinstance(other, PickleableCustom) and self.val == other.val
