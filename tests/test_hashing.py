from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import date
from typing import Any

from cashet import Client
from cashet.hashing import (
    ClosureWarning,
    UnhashableArgWarning,
    _ast_canonical,
    hash_args,
    hash_function,
)


class TestHashingEdgeCases:
    def test_unhashable_args_dict(self, client: Client) -> None:
        def process(data: dict[str, int]) -> int:
            return sum(data.values())

        ref = client.submit(process, {"a": 1, "b": 2, "c": 3})
        assert ref.load() == 6

    def test_unhashable_args_list(self, client: Client) -> None:
        def sum_list(nums: list[int]) -> int:
            return sum(nums)

        ref = client.submit(sum_list, [1, 2, 3, 4, 5])
        assert ref.load() == 15

    def test_unhashable_args_set(self, client: Client) -> None:
        def count_unique(items: set[str]) -> int:
            return len(items)

        ref = client.submit(count_unique, {"a", "b", "c"})
        assert ref.load() == 3

    def test_nested_unhashable(self, client: Client) -> None:
        def deep(data: dict[str, list[int]]) -> int:
            return sum(v for vals in data.values() for v in vals)

        ref = client.submit(deep, {"x": [1, 2], "y": [3, 4]})
        assert ref.load() == 10

    def test_function_with_closure(self, client: Client) -> None:
        base = 10

        def add_base(x: int) -> int:
            return x + base

        ref = client.submit(add_base, 5)
        assert ref.load() == 15

    def test_dict_ordering_deterministic(self, client: Client) -> None:
        def identity(x: Any) -> Any:
            return x

        d1 = {"a": 1, "b": 2, "c": 3}
        d2 = {"c": 3, "a": 1, "b": 2}
        ref1 = client.submit(identity, d1)
        ref2 = client.submit(identity, d2)
        assert ref1.hash == ref2.hash

    def test_mixed_types_in_args(self, client: Client) -> None:
        def mixed(a: int, b: str, c: float, d: bool) -> str:
            return f"{a}-{b}-{c}-{d}"

        ref = client.submit(mixed, 42, "hello", 3.14, True)
        assert ref.load() == "42-hello-3.14-True"

    def test_custom_object_arg_hash_includes_module(self, client: Client) -> None:
        thing_a = type("Thing", (), {})
        thing_a.__module__ = "module_a"
        thing_b = type("Thing", (), {})
        thing_b.__module__ = "module_b"
        a = thing_a()
        b = thing_b()
        a.v = 1
        b.v = 1

        def module_name(x: object) -> str:
            return x.__class__.__module__

        ref1 = client.submit(module_name, a)
        ref2 = client.submit(module_name, b)
        assert ref1.hash != ref2.hash
        assert ref1.load() == "module_a"
        assert ref2.load() == "module_b"

    def test_closure_values_not_in_cache_key(self, client: Client) -> None:
        base = 10

        def add_base(x: int) -> int:
            return x + base

        ref1 = client.submit(add_base, 5)
        assert ref1.load() == 15

        base = 20
        ref2 = client.submit(add_base, 5)
        assert ref2.load() == 15

        assert ref1.hash == ref2.hash

    def test_pass_explicit_args_for_cache_invalidation(self, client: Client) -> None:
        def add_base(x: int, base: int) -> int:
            return x + base

        ref1 = client.submit(add_base, 5, 10)
        assert ref1.load() == 15

        ref2 = client.submit(add_base, 5, 20)
        assert ref2.load() == 25

        assert ref1.hash != ref2.hash

    def test_default_values_are_in_function_hash(self, client: Client) -> None:
        def make(base: int) -> Any:
            def f(x: int = base) -> int:
                return x

            return f

        ref1 = client.submit(make(1))
        ref2 = client.submit(make(2))
        assert ref1.hash != ref2.hash
        assert ref1.load() == 1
        assert ref2.load() == 2


class TestObjectStateHashing:
    def test_slotted_objects_with_equal_state_hash_equal(self) -> None:
        class Point:
            __slots__ = ("x", "y")

            def __init__(self, x: int, y: int) -> None:
                self.x = x
                self.y = y

        assert hash_args(Point(1, 2)) == hash_args(Point(1, 2))

    def test_slotted_objects_with_different_state_hash_differ(self) -> None:
        class Point:
            __slots__ = ("x", "y")

            def __init__(self, x: int, y: int) -> None:
                self.x = x
                self.y = y

        assert hash_args(Point(1, 2)) != hash_args(Point(1, 3))

    def test_dataclass_with_slots_hashes_by_value(self) -> None:
        @dataclass(slots=True)
        class Config:
            name: str
            limit: int

        assert hash_args(Config("a", 1)) == hash_args(Config("a", 1))
        assert hash_args(Config("a", 1)) != hash_args(Config("a", 2))

    def test_mixed_dict_and_slots_state_both_hashed(self) -> None:
        class Base:
            __slots__ = ("__dict__", "a")

            def __init__(self, a: int, b: int) -> None:
                self.a = a
                self.b = b

        assert hash_args(Base(1, 2)) == hash_args(Base(1, 2))
        assert hash_args(Base(1, 2)) != hash_args(Base(1, 9))

    def test_opaque_object_warns(self) -> None:
        class Opaque:
            __slots__ = ()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            hash_args(Opaque())
        unhashable = [x for x in w if issubclass(x.category, UnhashableArgWarning)]
        assert len(unhashable) >= 1

    def test_custom_repr_object_does_not_warn(self) -> None:
        class Stable:
            __slots__ = ()

            def __repr__(self) -> str:
                return "Stable()"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            hash_args(Stable())
        unhashable = [x for x in w if issubclass(x.category, UnhashableArgWarning)]
        assert len(unhashable) == 0


class TestASTNormalizedHashing:
    def test_comment_stripped(self) -> None:
        src1 = "def foo(x):\n    # important\n    return x + 1"
        src2 = "def foo(x):\n    return x + 1"
        assert _ast_canonical(src1) == _ast_canonical(src2)

    def test_whitespace_normalized(self) -> None:
        src1 = "def foo(x):\n    return x + 1"
        src2 = "def  foo( x ):\n    return  x  +  1"
        assert _ast_canonical(src1) == _ast_canonical(src2)

    def test_semantic_change_detected(self) -> None:
        src1 = "def foo(x):\n    return x + 1"
        src2 = "def foo(x):\n    return x + 2"
        assert _ast_canonical(src1) != _ast_canonical(src2)

    def test_docstring_stripped(self) -> None:
        src1 = 'def foo(x):\n    """original"""\n    return x'
        src2 = 'def foo(x):\n    """updated docs"""\n    return x'
        assert _ast_canonical(src1) == _ast_canonical(src2)

    def test_only_docstring_function_does_not_crash(self) -> None:
        src = 'def f():\n    """only docs"""'
        out = _ast_canonical(src)
        assert "only docs" not in out
        assert _ast_canonical(src) == out

    def test_canonical_form_is_parseable_source(self) -> None:
        import ast

        out = _ast_canonical("def f(x):\n    # note\n    return x+1")
        # ast.unparse yields source text (round-trippable), unlike ast.dump.
        ast.parse(out)

    def test_different_functions_different_hash(self) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        def mul(x: int, y: int) -> int:
            return x * y

        assert hash_function(add) != hash_function(mul)

    def test_same_function_same_hash(self) -> None:
        def double(x: int) -> int:
            return x * 2

        assert hash_function(double) == hash_function(double)


class TestClosureWarning:
    def test_non_function_closure_emits_warning(self) -> None:
        base = 10

        def add_base(x: int) -> int:
            return x + base

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            hash_function(add_base)
        closure_warnings = [x for x in w if issubclass(x.category, ClosureWarning)]
        assert len(closure_warnings) == 1
        assert "base" in str(closure_warnings[0].message)

    def test_no_closure_no_warning(self) -> None:
        def pure(x: int) -> int:
            return x + 1

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            hash_function(pure)
        closure_warnings = [x for x in w if issubclass(x.category, ClosureWarning)]
        assert len(closure_warnings) == 0

    def test_function_closure_no_warning(self) -> None:
        def helper(x: int) -> int:
            return x * 2

        def uses_func(x: int) -> int:
            return helper(x)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            hash_function(uses_func)
        closure_warnings = [x for x in w if issubclass(x.category, ClosureWarning)]
        assert len(closure_warnings) == 0


class TestProgressiveHash:
    def test_large_list_same_hash(self, client: Client) -> None:
        def identity(data: list[int]) -> list[int]:
            return data

        large = list(range(100000))
        ref1 = client.submit(identity, large)
        ref2 = client.submit(identity, large)
        assert ref1.hash == ref2.hash

    def test_large_dict_same_hash(self, client: Client) -> None:
        def identity(data: dict[str, int]) -> dict[str, int]:
            return data

        large = {f"key_{i}": i for i in range(100000)}
        ref1 = client.submit(identity, large)
        ref2 = client.submit(identity, large)
        assert ref1.hash == ref2.hash

    def test_different_large_lists_different_hash(self, client: Client) -> None:
        def identity(data: list[int]) -> list[int]:
            return data

        ref1 = client.submit(identity, list(range(100000)))
        ref2 = client.submit(identity, list(range(100001)))
        assert ref1.hash != ref2.hash

    def test_str_no_collision_with_concatenated_empty(self, client: Client) -> None:
        def identity(data: list[str]) -> list[str]:
            return data

        ref1 = client.submit(identity, ["S"])
        ref2 = client.submit(identity, ["", ""])
        assert ref1.hash != ref2.hash
        assert ref1.load() != ref2.load()

    def test_dict_no_collision_on_swapped_kv(self, client: Client) -> None:
        def identity(data: dict[str, str]) -> dict[str, str]:
            return data

        ref1 = client.submit(identity, {"": "S"})
        ref2 = client.submit(identity, {"S": ""})
        assert ref1.hash != ref2.hash

    def test_bytes_no_collision_with_split(self, client: Client) -> None:
        def identity(data: list[bytes]) -> list[bytes]:
            return data

        ref1 = client.submit(identity, [b"AB"])
        ref2 = client.submit(identity, [b"A", b"B"])
        assert ref1.hash != ref2.hash

    def test_set_dict_no_collision(self, client: Client) -> None:
        def identity(data: Any) -> Any:
            return data

        ref1 = client.submit(identity, {1, 2})
        ref2 = client.submit(identity, {1: 2})
        assert ref1.hash != ref2.hash

    def test_set_hash_ignores_item_repr_order(self) -> None:
        reprs: dict[int, str] = {}

        class SlottedVal:
            __slots__ = ("val",)

            def __init__(self, val: int) -> None:
                self.val = val

            def __repr__(self) -> str:
                return reprs[id(self)]

        a1, b1 = SlottedVal(1), SlottedVal(2)
        a2, b2 = SlottedVal(1), SlottedVal(2)
        reprs[id(a1)], reprs[id(b1)] = "0", "1"
        reprs[id(a2)], reprs[id(b2)] = "1", "0"
        assert hash_args({a1, b1}) == hash_args({a2, b2})
        assert hash_args(frozenset({a1, b1})) == hash_args(frozenset({a2, b2}))


class TestRecursiveStructures:
    def test_recursive_list_does_not_crash(self, client: Client) -> None:
        def identity(data: list[Any]) -> list[Any]:
            return data

        a = [1, 2]
        a.append(a)
        ref = client.submit(identity, a)
        result = ref.load()
        assert result[0] == 1
        assert result[1] == 2
        assert result[2] is result

    def test_recursive_dict_does_not_crash(self, client: Client) -> None:
        def identity(data: dict[str, Any]) -> dict[str, Any]:
            return data

        d = {"x": 1}
        d["self"] = d
        ref = client.submit(identity, d)
        result = ref.load()
        assert result["x"] == 1
        assert result["self"] is result

    def test_same_recursive_structure_same_hash(self, client: Client) -> None:
        def identity(data: list[Any]) -> list[Any]:
            return data

        a = [1, 2]
        a.append(a)
        b = [1, 2]
        b.append(b)
        ref1 = client.submit(identity, a)
        ref2 = client.submit(identity, b)
        assert ref1.hash == ref2.hash


class TestRecursiveGlobalHashing:
    def test_helper_change_invalidates_caller_hash(self) -> None:
        def helper_v1(x: int) -> int:
            return x + 1

        def caller_v1(x: int) -> int:
            return helper_v1(x)

        def helper_v2(x: int) -> int:
            return x + 2

        def caller_v2(x: int) -> int:
            return helper_v2(x)

        assert hash_function(caller_v1) != hash_function(caller_v2)

    def test_same_helper_same_caller_hash(self) -> None:
        def helper(x: int) -> int:
            return x + 1

        def caller(x: int) -> int:
            return helper(x)

        assert hash_function(caller) == hash_function(caller)

    def test_builtin_function_not_included_in_hash(self) -> None:
        def caller(data: list[int]) -> list[int]:
            return sorted(data)

        hash1 = hash_function(caller)
        hash2 = hash_function(caller)
        assert hash1 == hash2

    def test_caller_hash_includes_transitive_helpers(self) -> None:
        def leaf(x: int) -> int:
            return x * 2

        def mid(x: int) -> int:
            return leaf(x) + 1

        def top(x: int) -> int:
            return mid(x)

        hash1 = hash_function(top)

        def leaf_v2(x: int) -> int:
            return x * 3

        def mid_v2(x: int) -> int:
            return leaf_v2(x) + 1

        def top_v2(x: int) -> int:
            return mid_v2(x)

        hash2 = hash_function(top_v2)
        assert hash1 != hash2




class TestDynamicSource:
    def test_exec_function_hashes_by_bytecode(self, client: Client) -> None:
        code = "def dynamic_func(x):\n    return x + 1"
        namespace: dict[str, Any] = {}
        exec(code, namespace)
        func = namespace["dynamic_func"]
        ref1 = client.submit(func, 5)
        assert ref1.load() == 6

        # Same code should cache
        ref2 = client.submit(func, 5)
        assert ref1.hash == ref2.hash

    def test_exec_function_invalidates_on_change(self, client: Client) -> None:
        namespace1: dict[str, Any] = {}
        exec("def f(x):\n    return x + 1", namespace1)
        ref1 = client.submit(namespace1["f"], 5)

        namespace2: dict[str, Any] = {}
        exec("def f(x):\n    return x + 2", namespace2)
        ref2 = client.submit(namespace2["f"], 5)

        assert ref1.hash != ref2.hash
        assert ref1.load() == 6
        assert ref2.load() == 7

    def test_exec_function_invalidates_on_default_change(self, client: Client) -> None:
        namespace1: dict[str, Any] = {}
        exec("def f(x=1):\n    return x", namespace1)
        ref1 = client.submit(namespace1["f"])

        namespace2: dict[str, Any] = {}
        exec("def f(x=2):\n    return x", namespace2)
        ref2 = client.submit(namespace2["f"])

        assert ref1.hash != ref2.hash
        assert ref1.load() == 1
        assert ref2.load() == 2

    def test_exec_function_invalidates_on_global_name_change(self, client: Client) -> None:
        namespace1: dict[str, Any] = {"A": 1}
        exec("def f():\n    return A", namespace1)
        ref1 = client.submit(namespace1["f"])

        namespace2: dict[str, Any] = {"B": 2}
        exec("def f():\n    return B", namespace2)
        ref2 = client.submit(namespace2["f"])

        assert ref1.hash != ref2.hash
        assert ref1.load() == 1
        assert ref2.load() == 2

    def test_exec_function_invalidates_on_global_value_change(self, client: Client) -> None:
        namespace: dict[str, Any] = {"MULTIPLIER": 2}
        exec("def f(x):\n    return x * MULTIPLIER", namespace)
        func = namespace["f"]
        ref1 = client.submit(func, 10)

        namespace["MULTIPLIER"] = 3
        ref2 = client.submit(func, 10)

        assert ref1.hash != ref2.hash
        assert ref1.load() == 20
        assert ref2.load() == 30

    def test_exec_function_invalidates_on_dict_global_change(self, client: Client) -> None:
        namespace: dict[str, Any] = {"CONFIG": {"factor": 2}}
        exec("def f(x):\n    return x * CONFIG['factor']", namespace)
        func = namespace["f"]
        ref1 = client.submit(func, 10)

        namespace["CONFIG"] = {"factor": 3}
        ref2 = client.submit(func, 10)

        assert ref1.hash != ref2.hash
        assert ref1.load() == 20
        assert ref2.load() == 30

    def test_exec_function_invalidates_on_list_global_change(self, client: Client) -> None:
        namespace: dict[str, Any] = {"WEIGHTS": [1, 2]}
        exec("def f():\n    return sum(WEIGHTS)", namespace)
        func = namespace["f"]
        ref1 = client.submit(func)

        namespace["WEIGHTS"] = [1, 2, 3]
        ref2 = client.submit(func)

        assert ref1.hash != ref2.hash
        assert ref1.load() == 3
        assert ref2.load() == 6

    def test_global_container_with_unstable_member_not_hashed(self, client: Client) -> None:
        namespace: dict[str, Any] = {"REGISTRY": {"handler": object()}}
        exec("def f():\n    return len(REGISTRY)", namespace)
        func = namespace["f"]
        ref1 = client.submit(func)

        namespace["REGISTRY"] = {"handler": object()}
        ref2 = client.submit(func)

        assert ref1.hash == ref2.hash
        assert ref1.load() == 1

    def test_comprehension_invalidates_on_global_value_change(self, client: Client) -> None:
        namespace: dict[str, Any] = {"MULTIPLIER": 2}
        exec("def f(xs):\n    return [x * MULTIPLIER for x in xs]", namespace)
        func = namespace["f"]
        ref1 = client.submit(func, [10])

        namespace["MULTIPLIER"] = 3
        ref2 = client.submit(func, [10])

        assert ref1.hash != ref2.hash
        assert ref1.load() == [20]
        assert ref2.load() == [30]

    def test_exec_function_invalidates_on_range_global_change(self, client: Client) -> None:
        namespace: dict[str, Any] = {"WINDOW": range(2)}
        exec("def f():\n    return list(WINDOW)", namespace)
        func = namespace["f"]
        ref1 = client.submit(func)

        namespace["WINDOW"] = range(3)
        ref2 = client.submit(func)

        assert ref1.hash != ref2.hash
        assert ref1.load() == [0, 1]
        assert ref2.load() == [0, 1, 2]

    def test_exec_function_invalidates_on_slice_global_change(self, client: Client) -> None:
        namespace: dict[str, Any] = {"PART": slice(0, 2)}
        exec("def f(xs):\n    return xs[PART]", namespace)
        func = namespace["f"]
        ref1 = client.submit(func, [1, 2, 3])

        namespace["PART"] = slice(1, 3)
        ref2 = client.submit(func, [1, 2, 3])

        assert ref1.hash != ref2.hash
        assert ref1.load() == [1, 2]
        assert ref2.load() == [2, 3]

    def test_exec_function_invalidates_on_date_global_change(self, client: Client) -> None:
        namespace: dict[str, Any] = {"START": date(2026, 5, 11)}
        exec("def f():\n    return START.isoformat()", namespace)
        func = namespace["f"]
        ref1 = client.submit(func)

        namespace["START"] = date(2026, 5, 12)
        ref2 = client.submit(func)

        assert ref1.hash != ref2.hash
        assert ref1.load() == "2026-05-11"
        assert ref2.load() == "2026-05-12"

    def test_lambda_hashes_by_bytecode(self, client: Client) -> None:
        f = lambda x: x * 3  # noqa: E731
        ref1 = client.submit(f, 4)
        ref2 = client.submit(f, 4)
        assert ref1.hash == ref2.hash
        assert ref1.load() == 12

    def test_different_lambdas_different_hashes(self, client: Client) -> None:
        f1 = lambda x: x * 3  # noqa: E731
        f2 = lambda x: x * 4  # noqa: E731
        ref1 = client.submit(f1, 4)
        ref2 = client.submit(f2, 4)
        assert ref1.hash != ref2.hash
