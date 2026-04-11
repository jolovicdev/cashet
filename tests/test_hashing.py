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


