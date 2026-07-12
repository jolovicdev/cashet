from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

_pickle_warning_issued = False


def warn_default_pickle() -> None:
    global _pickle_warning_issued
    if _pickle_warning_issued:
        return
    _pickle_warning_issued = True
    import warnings

    warnings.warn(
        "Using PickleSerializer by default — arbitrary code execution risk on "
        "untrusted cached results. Pass serializer=SafePickleSerializer() "
        "for safer deserialization.",
        stacklevel=3,
    )


@runtime_checkable
class Serializer(Protocol):
    def dumps(self, obj: Any) -> bytes: ...
    def loads(self, data: bytes) -> Any: ...


class PickleSerializer:
    def __init__(self, protocol: int | None = None) -> None:
        import pickle

        self._pickle = pickle
        self._protocol = protocol or pickle.HIGHEST_PROTOCOL

    def dumps(self, obj: Any) -> bytes:
        return self._pickle.dumps(obj, protocol=self._protocol)

    def loads(self, data: bytes) -> Any:
        return self._pickle.loads(data)


class JsonSerializer:
    def dumps(self, obj: Any) -> bytes:
        import json

        return json.dumps(obj, default=str, sort_keys=True).encode()

    def loads(self, data: bytes) -> Any:
        import json

        return json.loads(data)


class SafePickleSerializer:
    _cached_allowlist: list[type] | None = None

    def __init__(self, extra_classes: list[type] | None = None) -> None:
        import pickle

        self._pickle = pickle
        self._allowed: dict[str, type] = {}
        for cls in self._default_allowlist():
            key = f"{cls.__module__}.{cls.__qualname__}"
            self._allowed[key] = cls
        if extra_classes:
            for cls in extra_classes:
                key = f"{cls.__module__}.{cls.__qualname__}"
                self._allowed[key] = cls

    def dumps(self, obj: Any) -> bytes:
        return self._pickle.dumps(obj, protocol=self._pickle.HIGHEST_PROTOCOL)

    def loads(self, data: bytes) -> Any:
        import io
        import pickle

        allowed = self._allowed
        blocked_msg = " — not in allowlist. Pass it via SafePickleSerializer(extra_classes=[...])."

        class _RestrictedUnpickler(pickle.Unpickler):
            def find_class(self, module: str, name: str) -> Any:  # type: ignore[override]
                key = f"{module}.{name}"
                if key in allowed:
                    return allowed[key]
                raise pickle.UnpicklingError(f"Blocked class {key}{blocked_msg}")

        return _RestrictedUnpickler(io.BytesIO(data)).load()

    @classmethod
    def _default_allowlist(cls) -> list[type]:
        if cls._cached_allowlist is not None:
            return cls._cached_allowlist
        import collections
        import datetime

        types_list: list[type] = [
            type(None),
            bool,
            int,
            float,
            str,
            bytes,
            bytearray,
            list,
            dict,
            tuple,
            set,
            frozenset,
            slice,
            range,
            complex,
            object,
            type,
            datetime.datetime,
            datetime.date,
            datetime.timedelta,
            datetime.time,
            datetime.timezone,
            collections.OrderedDict,
            collections.defaultdict,
            collections.Counter,
            collections.deque,
        ]
        try:
            import numpy  # pyright: ignore[reportMissingImports]

            types_list.append(numpy.ndarray)  # type: ignore[attr-defined]
        except ImportError:
            pass
        cls._cached_allowlist = types_list
        return types_list
