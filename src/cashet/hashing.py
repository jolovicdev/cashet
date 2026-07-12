from __future__ import annotations

import ast
import contextlib
import datetime as _datetime
import hashlib
import inspect
import io
import os
import site
import sys
import textwrap
import types
import warnings
import weakref
from datetime import timedelta
from functools import lru_cache
from typing import Any

from cashet.models import TaskDef
from cashet.serializers import JsonSerializer as JsonSerializer
from cashet.serializers import PickleSerializer as PickleSerializer
from cashet.serializers import SafePickleSerializer as SafePickleSerializer
from cashet.serializers import Serializer as Serializer
from cashet.serializers import warn_default_pickle as warn_default_pickle


class ClosureWarning(UserWarning):
    pass


class UnhashableArgWarning(UserWarning):
    pass


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


# Keyed by the function object itself: a redefinition (new cell, reloaded
# module) is a new object and misses, while repeat submissions of the same
# function skip the source lookup and its file IO entirely.
_source_cache: weakref.WeakKeyDictionary[types.FunctionType, str] = weakref.WeakKeyDictionary()


def get_func_source(func: types.FunctionType) -> str:
    cached = _source_cache.get(func)
    if cached is not None:
        return cached
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
    normalized = _normalize_source(source)
    with contextlib.suppress(TypeError):
        _source_cache[func] = normalized
    return normalized


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


@lru_cache(maxsize=1024)
def _ast_canonical(source: str) -> str:
    # ast.unparse normalizes whitespace and comments like ast.dump but, being
    # source text rather than the internal AST repr, stays stable across Python
    # versions whose ast.dump field set differs (e.g. type_params in 3.12).
    try:
        tree = ast.parse(source)
        _strip_docstrings(tree)
        return ast.unparse(tree)
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
            if not child.body:
                child.body = [ast.Pass()]


@lru_cache(maxsize=1)
def _stdlib_and_site_prefixes() -> tuple[str, ...]:
    stdlib_path = os.path.abspath(os.path.dirname(os.__file__))
    site_paths = [os.path.abspath(p) for p in site.getsitepackages() if p]
    user_site = site.getusersitepackages()
    if user_site:
        site_paths.append(os.path.abspath(user_site))
    return (stdlib_path, *site_paths)


@lru_cache(maxsize=4096)
def _is_stdlib_or_site_path(path: str) -> bool:
    resolved = os.path.abspath(path)
    for prefix in _stdlib_and_site_prefixes():
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


_HASHED_GLOBAL_TYPES = (
    type(None),
    bool,
    int,
    float,
    str,
    bytes,
    complex,
    range,
    _datetime.date,
    _datetime.datetime,
    _datetime.time,
    _datetime.timedelta,
    _datetime.timezone,
)


def _should_hash_global_value(obj: Any, visited: set[int] | None = None) -> bool:
    if isinstance(obj, _HASHED_GLOBAL_TYPES):
        return True
    if visited is None:
        visited = set()
    obj_id = id(obj)
    if obj_id in visited:
        return False
    if isinstance(obj, slice):
        visited.add(obj_id)
        result = all(
            _should_hash_global_value(item, visited)
            for item in (obj.start, obj.stop, obj.step)
        )
        visited.discard(obj_id)
        return result
    if isinstance(obj, tuple | frozenset | list | set):
        visited.add(obj_id)
        result = all(_should_hash_global_value(item, visited) for item in obj)
        visited.discard(obj_id)
        return result
    if isinstance(obj, dict):
        visited.add(obj_id)
        result = all(
            _should_hash_global_value(k, visited) and _should_hash_global_value(v, visited)
            for k, v in obj.items()
        )
        visited.discard(obj_id)
        return result
    return False


def _code_names(code: types.CodeType, visited: set[int] | None = None) -> set[str]:
    if visited is None:
        visited = set()
    code_id = id(code)
    if code_id in visited:
        return set()
    visited.add(code_id)
    names = set(code.co_names)
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            names.update(_code_names(const, visited))
    return names


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
    for name in sorted(_code_names(func.__code__)):
        if name not in func.__globals__:
            continue
        ref = func.__globals__[name]
        if isinstance(ref, types.FunctionType) and _is_user_function(ref):
            dep_hash = hash_function(ref, include_deps=False, visited=visited)
            if dep_hash:
                h.update(f"{name}:{dep_hash}".encode())
        elif _should_hash_global_value(ref):
            h.update(f"<global:{name}>".encode())
            _stable_hash(ref, h)
    if non_func_closures:
        names = ", ".join(non_func_closures)
        warnings.warn(
            f"Closure variables [{names}] are not hashed — "
            f"pass them as explicit arguments for correct cache invalidation.",
            ClosureWarning,
            stacklevel=3,
        )
    return h.hexdigest()


@lru_cache(maxsize=1024)
def _slot_names(cls: type) -> tuple[str, ...]:
    names: list[str] = []
    for klass in cls.__mro__:
        slots = klass.__dict__.get("__slots__")
        if slots is None:
            continue
        if isinstance(slots, str):
            slots = (slots,)
        for name in slots:
            if name in ("__dict__", "__weakref__"):
                continue
            names.append(name)
    return tuple(names)


_UNSET = object()


def object_state(obj: Any) -> dict[str, Any] | None:
    state: dict[str, Any] = {}
    instance_dict = getattr(obj, "__dict__", None)
    if isinstance(instance_dict, dict):
        state.update(instance_dict)
    for name in _slot_names(type(obj)):
        value = getattr(obj, name, _UNSET)
        if value is not _UNSET:
            state[name] = value
    return state or None


def _stable_item_reprs(items: Any, _visited: set[int]) -> list[str]:
    # Set ordering must come from the items' stable serialized form; raw repr
    # can embed memory addresses, which reorder across processes.
    reprs: list[str] = []
    for item in items:
        sub = io.StringIO()
        _stable_repr_to(sub, item, _visited)
        reprs.append(sub.getvalue())
    return reprs


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
        buf.write(", ".join(sorted(_stable_item_reprs(obj, _visited))))
        buf.write("}")
        _visited.discard(obj_id)
    elif isinstance(obj, frozenset):
        obj_id = id(obj)
        if obj_id in _visited:
            buf.write("frozenset({...})")
            return
        _visited.add(obj_id)
        buf.write("frozenset({")
        buf.write(", ".join(sorted(_stable_item_reprs(obj, _visited))))
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
    else:
        state = object_state(obj)
        if state is not None:
            obj_id = id(obj)
            if obj_id in _visited:
                buf.write(f"<{type(obj).__module__}.{type(obj).__qualname__}:...>")
                return
            _visited.add(obj_id)
            buf.write(f"<{type(obj).__module__}.{type(obj).__qualname__}:")
            _stable_repr_to(buf, state, _visited)
            buf.write(">")
            _visited.discard(obj_id)
        else:
            if type(obj).__repr__ is object.__repr__:
                warnings.warn(
                    f"Argument of type "
                    f"{type(obj).__module__}.{type(obj).__qualname__} has no "
                    f"__dict__/__slots__ and uses the default repr; it cannot be "
                    f"hashed by value and will not cache reliably. Pass a "
                    f"value-stable representation instead.",
                    UnhashableArgWarning,
                    stacklevel=2,
                )
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
