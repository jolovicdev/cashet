from __future__ import annotations

import ast
import hashlib
import inspect
import io
import os
import site
import sys
import textwrap
import types
import warnings
from datetime import timedelta
from typing import Any, Protocol, runtime_checkable

from cashet.models import TaskDef


class ClosureWarning(UserWarning):
    pass


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


def _normalize_source(source: str) -> str:
    return textwrap.dedent(source).strip()


def _bytecode_source(func: types.FunctionType) -> str:
    code = func.__code__
    return (
        f"<bytecode:{func.__qualname__}:{code.co_argcount}:"
        f"{code.co_posonlyargcount}:{code.co_kwonlyargcount}:{code.co_nlocals}:"
        f"{code.co_flags}:{code.co_code.hex()}:{code.co_consts!r}:"
        f"{code.co_names!r}:{code.co_varnames!r}:{code.co_cellvars!r}:"
        f"{code.co_freevars!r}:{func.__defaults__!r}:{func.__kwdefaults__!r}>"
    )


def get_func_source(func: types.FunctionType) -> str:
    try:
        source = inspect.getsource(func)
    except OSError:
        source = None
        try:
            import dill  # type: ignore[reportMissingTypeStubs]

            source = dill.source.getsource(func)  # type: ignore[reportUnknownMemberType]
        except Exception:
            pass
        if source is None:
            source = _bytecode_source(func)
    return _normalize_source(source)


def get_dep_versions(func: types.FunctionType) -> dict[str, str]:
    module = inspect.getmodule(func)
    if module is None:
        return {}
    refs: dict[str, str] = {}
    mod_name = module.__name__
    top = mod_name.split(".")[0]
    try:
        mod = sys.modules.get(top)
        if mod and hasattr(mod, "__version__"):
            refs[top] = mod.__version__
    except Exception:
        pass
    return refs


def hash_source(source: str) -> str:
    return hashlib.sha256(source.encode()).hexdigest()


def _ast_canonical(source: str) -> str:
    try:
        tree = ast.parse(source)
        _strip_docstrings(tree)
        return ast.dump(tree)
    except SyntaxError:
        return source


_DOCSTRING_CARRYING = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)


def _strip_docstrings(node: ast.AST) -> None:
    for child in ast.walk(node):
        if not isinstance(child, _DOCSTRING_CARRYING):
            continue
        if (
            child.body
            and isinstance(child.body[0], ast.Expr)
            and isinstance(child.body[0].value, ast.Constant)
            and isinstance(child.body[0].value.value, str)
        ):
            child.body = child.body[1:]


def _is_stdlib_or_site_path(path: str) -> bool:
    resolved = os.path.abspath(path)
    stdlib_path = os.path.abspath(os.path.dirname(os.__file__))
    site_paths = [os.path.abspath(p) for p in site.getsitepackages() if p]
    user_site = site.getusersitepackages()
    if user_site:
        site_paths.append(os.path.abspath(user_site))
    excluded = [stdlib_path, *site_paths]
    for prefix in excluded:
        try:
            if os.path.commonpath([resolved, prefix]) == prefix:
                return True
        except ValueError:
            pass
    return False


def _is_user_function(func: types.FunctionType) -> bool:
    mod = inspect.getmodule(func)
    if mod is None:
        return False
    mod_name = mod.__name__
    if mod_name == "__main__":
        return True
    if mod_name in sys.builtin_module_names:
        return False
    mod_file = getattr(mod, "__file__", None)
    if mod_file is None:
        return False
    return not _is_stdlib_or_site_path(mod_file)


def hash_function(
    func: types.FunctionType,
    include_deps: bool = True,
    visited: set[int] | None = None,
) -> str:
    if visited is None:
        visited = set()
    func_id = id(func)
    if func_id in visited:
        return ""
    visited.add(func_id)

    h = hashlib.sha256()
    source = get_func_source(func)
    h.update(hash_source(_ast_canonical(source)).encode())
    if func.__defaults__ is not None:
        h.update(b"<defaults>")
        _stable_hash(func.__defaults__, h)
    if func.__kwdefaults__ is not None:
        h.update(b"<kwdefaults>")
        _stable_hash(func.__kwdefaults__, h)
    if include_deps:
        deps = get_dep_versions(func)
        for name in sorted(deps):
            h.update(f"{name}=={deps[name]}".encode())
    non_func_closures: list[str] = []
    if hasattr(func, "__closure__") and func.__closure__:
        freevars = func.__code__.co_freevars
        for i, cell in enumerate(func.__closure__):
            try:
                cell_content = cell.cell_contents
                if isinstance(cell_content, types.FunctionType):
                    h.update(
                        hash_function(cell_content, include_deps=False, visited=visited).encode()
                    )
                else:
                    name = freevars[i] if i < len(freevars) else f"<closure_{i}>"
                    non_func_closures.append(name)
            except ValueError:
                pass
    for name in sorted(func.__code__.co_names):
        ref = func.__globals__.get(name)
        if isinstance(ref, types.FunctionType) and _is_user_function(ref):
            dep_hash = hash_function(ref, include_deps=False, visited=visited)
            if dep_hash:
                h.update(f"{name}:{dep_hash}".encode())
    if non_func_closures:
        names = ", ".join(non_func_closures)
        warnings.warn(
            f"Closure variables [{names}] are not hashed — "
            f"pass them as explicit arguments for correct cache invalidation.",
            ClosureWarning,
            stacklevel=3,
        )
    return h.hexdigest()


def _stable_repr_to(
    buf: io.StringIO, obj: Any, _visited: set[int] | None = None
) -> None:
    if _visited is None:
        _visited = set()
    if obj is None:
        buf.write("None")
    elif isinstance(obj, (bool, int, float, str, bytes)):
        buf.write(repr(obj))
    elif isinstance(obj, (list, tuple)):
        obj_id = id(obj)
        if obj_id in _visited:
            buf.write("[...]" if isinstance(obj, list) else "(...)")
            return
        _visited.add(obj_id)
        buf.write("[" if isinstance(obj, list) else "(")
        first = True
        for item in obj:
            if not first:
                buf.write(", ")
            first = False
            _stable_repr_to(buf, item, _visited)
        buf.write("]" if isinstance(obj, list) else ")")
        _visited.discard(obj_id)
    elif isinstance(obj, set):
        obj_id = id(obj)
        if obj_id in _visited:
            buf.write("{...}")
            return
        _visited.add(obj_id)
        buf.write("{")
        first = True
        for item in sorted(obj, key=repr):
            if not first:
                buf.write(", ")
            first = False
            _stable_repr_to(buf, item, _visited)
        buf.write("}")
        _visited.discard(obj_id)
    elif isinstance(obj, frozenset):
        obj_id = id(obj)
        if obj_id in _visited:
            buf.write("frozenset({...})")
            return
        _visited.add(obj_id)
        buf.write("frozenset({")
        first = True
        for item in sorted(obj, key=repr):
            if not first:
                buf.write(", ")
            first = False
            _stable_repr_to(buf, item, _visited)
        buf.write("})")
        _visited.discard(obj_id)
    elif isinstance(obj, dict):
        obj_id = id(obj)
        if obj_id in _visited:
            buf.write("{...}")
            return
        _visited.add(obj_id)
        buf.write("{")
        first = True
        for key, val in sorted(obj.items(), key=lambda p: repr(p[0])):
            if not first:
                buf.write(", ")
            first = False
            _stable_repr_to(buf, key, _visited)
            buf.write(": ")
            _stable_repr_to(buf, val, _visited)
        buf.write("}")
        _visited.discard(obj_id)
    elif isinstance(obj, types.FunctionType):
        buf.write(f"<func:{hash_function(obj)}>")
    elif isinstance(obj, type):
        buf.write(f"<type:{obj.__module__}.{obj.__qualname__}>")
    elif hasattr(obj, "__cashet_ref__"):
        buf.write(f"<ref:{obj.__cashet_ref__().hash}>")
    elif hasattr(obj, "__dict__"):
        obj_id = id(obj)
        if obj_id in _visited:
            buf.write(f"<{type(obj).__module__}.{type(obj).__qualname__}:...>")
            return
        _visited.add(obj_id)
        buf.write(f"<{type(obj).__module__}.{type(obj).__qualname__}:")
        _stable_repr_to(buf, obj.__dict__, _visited)
        buf.write(">")
        _visited.discard(obj_id)
    else:
        buf.write(repr(obj))


def _stable_hash(
    obj: Any, h: Any, _visited: set[int] | None = None
) -> None:
    buf = io.StringIO()
    _stable_repr_to(buf, obj, _visited)
    h.update(buf.getvalue().encode())


def hash_args(*args: Any, **kwargs: Any) -> str:
    h = hashlib.sha256()
    _stable_hash((args, kwargs), h)
    return h.hexdigest()


def serialize_args(*args: Any, **kwargs: Any) -> bytes:
    buf = io.StringIO()
    _stable_repr_to(buf, (args, kwargs))
    return buf.getvalue().encode()


def build_task_def(
    func: types.FunctionType,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    cache: bool = True,
    tags: dict[str, str] | None = None,
    retries: int = 0,
    force: bool = False,
    timeout: timedelta | None = None,
    ttl: timedelta | None = None,
) -> TaskDef:
    func_hash = hash_function(func)
    args_hash_val = hash_args(*args, **kwargs)
    args_snapshot = serialize_args(*args, **kwargs)
    source = get_func_source(func)
    dep_versions = get_dep_versions(func)
    return TaskDef(
        func_hash=func_hash,
        func_name=getattr(func, "_cashet_name", func.__qualname__),
        func_source=source,
        args_hash=args_hash_val,
        args_snapshot=args_snapshot,
        dep_versions=dep_versions,
        cache=cache,
        tags=tags or {},
        retries=retries,
        force=force,
        timeout=timeout,
        ttl=ttl,
    )
