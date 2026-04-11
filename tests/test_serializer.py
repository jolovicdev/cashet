from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cashet import Client, PickleSerializer, SafePickleSerializer
from tests.helpers import PickleableCustom


class TestSerializer:
    def test_custom_serializer(self, store_dir: Path) -> None:
        from cashet.hashing import JsonSerializer

        client = Client(store_dir=store_dir, serializer=JsonSerializer())

        def greet(name: str) -> str:
            return f"hello, {name}"

        ref = client.submit(greet, "world")
        assert ref.load() == "hello, world"

    def test_pickle_serializer_default(self, store_dir: Path) -> None:
        client = Client(store_dir=store_dir, serializer=PickleSerializer())

        def make_complex() -> dict[str, list[int]]:
            return {"data": [1, 2, 3]}

        ref = client.submit(make_complex)
        assert ref.load() == {"data": [1, 2, 3]}


class TestSafePickleSerializer:
    def test_roundtrip_builtins(self) -> None:
        s = SafePickleSerializer()
        for obj in [42, 3.14, "hello", [1, 2], {"a": 1}, (1, 2), {1, 2}, True, None]:
            assert s.loads(s.dumps(obj)) == obj

    def test_rejects_unknown_class(self) -> None:
        import pickle
        from pathlib import Path

        s = SafePickleSerializer()
        data = pickle.dumps(Path("/tmp/test"))
        with pytest.raises(pickle.UnpicklingError, match="Blocked class"):
            s.loads(data)

    def test_extra_classes_allowed(self) -> None:
        s = SafePickleSerializer(extra_classes=[PickleableCustom])
        data = s.dumps(PickleableCustom(42))
        result = s.loads(data)
        assert isinstance(result, PickleableCustom)
        assert result.val == 42

    def test_datetime_roundtrip(self) -> None:
        from datetime import date

        s = SafePickleSerializer()
        now = datetime.now(UTC)
        assert s.loads(s.dumps(now)) == now
        assert s.loads(s.dumps(date(2024, 1, 1))) == date(2024, 1, 1)
        assert s.loads(s.dumps(timedelta(hours=2))) == timedelta(hours=2)


