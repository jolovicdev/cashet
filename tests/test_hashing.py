from __future__ import annotations

import warnings
from typing import Any

from cashet import Client
from cashet.hashing import ClosureWarning, _ast_canonical, hash_function


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
