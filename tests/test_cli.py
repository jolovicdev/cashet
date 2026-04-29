from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from cashet import Client
from cashet.cli import main


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def store_dir(tmp_path: Path) -> Path:
    d = tmp_path / ".cashet"
    d.mkdir()
    return d


def _invoke(cli_runner: CliRunner, args: list[str], store_dir: Path) -> Any:
    return cli_runner.invoke(main, args, env={"CASHET_DIR": str(store_dir)})


class TestLog:
    def test_log_empty(self, cli_runner: CliRunner, store_dir: Path) -> None:
        result = _invoke(cli_runner, ["log"], store_dir)
        assert result.exit_code == 0
        assert "No commits found" in result.output

    def test_log_invalid_tag(self, cli_runner: CliRunner, store_dir: Path) -> None:
        result = _invoke(cli_runner, ["log", "--tag", "=value"], store_dir)
        assert result.exit_code == 2
        assert "Invalid tag" in result.output or "key cannot be empty" in result.output

    def test_log_empty_tag_key(self, cli_runner: CliRunner, store_dir: Path) -> None:
        result = _invoke(cli_runner, ["log", "--tag", ""], store_dir)
        assert result.exit_code == 2
        assert "Tag key cannot be empty" in result.output or "key cannot be empty" in result.output

    def test_log_shows_commits(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def add(x: int, y: int) -> int:
            return x + y

        client.submit(add, 1, 2)
        result = _invoke(cli_runner, ["log"], store_dir)
        assert result.exit_code == 0
        assert "completed" in result.output

    def test_log_filter_by_func(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def foo() -> int:
            return 1

        def bar() -> int:
            return 2

        client.submit(foo)
        client.submit(bar)
        result = _invoke(cli_runner, ["log", "--func", foo.__qualname__], store_dir)
        assert result.exit_code == 0
        assert "completed" in result.output
        assert "bar" not in result.output

    def test_log_filter_by_status(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def boom() -> None:
            raise ValueError("kaboom")

        with contextlib.suppress(Exception):
            client.submit(boom)

        result = _invoke(cli_runner, ["log", "--status", "failed"], store_dir)
        assert result.exit_code == 0
        assert "failed" in result.output

    def test_log_invalid_status(self, cli_runner: CliRunner, store_dir: Path) -> None:
        result = _invoke(cli_runner, ["log", "--status", "nope"], store_dir)
        assert result.exit_code == 1
        assert "Invalid status" in result.output


class TestShow:
    def test_show_commit(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def greet(name: str) -> str:
            return f"hello {name}"

        ref = client.submit(greet, "world")
        result = _invoke(cli_runner, ["show", ref.commit_hash], store_dir)
        assert result.exit_code == 0
        assert "greet" in result.output
        assert "hello" in result.output

    def test_show_missing(self, cli_runner: CliRunner, store_dir: Path) -> None:
        result = _invoke(cli_runner, ["show", "deadbeef"], store_dir)
        assert result.exit_code == 0
        assert "not found" in result.output

    def test_show_ambiguous_hash(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)
        hashes: list[str] = []
        for i in range(20):

            def make_val(idx: int = i) -> int:
                return idx

            ref = client.submit(make_val, _cache=False)
            hashes.append(ref.commit_hash)
        from collections import Counter

        first_digits = Counter(h[0] for h in hashes)
        ambiguous = next(d for d, count in first_digits.items() if count > 1)
        result = _invoke(cli_runner, ["show", ambiguous], store_dir)
        assert result.exit_code == 1
        assert "Ambiguous prefix" in result.output


class TestGet:
    def test_get_pretty_print(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def val() -> dict[str, int]:
            return {"a": 1}

        ref = client.submit(val)
        result = _invoke(cli_runner, ["get", ref.commit_hash], store_dir)
        assert result.exit_code == 0
        assert '"a": 1' in result.output

    def test_get_write_file(self, cli_runner: CliRunner, store_dir: Path, tmp_path: Path) -> None:
        client = Client(store_dir=store_dir)

        def val() -> str:
            return "hello file"

        ref = client.submit(val)
        out_path = tmp_path / "output.bin"
        result = _invoke(cli_runner, ["get", ref.commit_hash, "-o", str(out_path)], store_dir)
        assert result.exit_code == 0
        assert client.serializer.loads(out_path.read_bytes()) == "hello file"

    def test_get_missing(self, cli_runner: CliRunner, store_dir: Path) -> None:
        result = _invoke(cli_runner, ["get", "deadbeef"], store_dir)
        assert result.exit_code == 0
        assert "No result" in result.output

    def test_get_ambiguous_hash(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)
        hashes: list[str] = []
        for i in range(20):

            def make_val(idx: int = i) -> int:
                return idx

            ref = client.submit(make_val, _cache=False)
            hashes.append(ref.commit_hash)
        from collections import Counter

        first_digits = Counter(h[0] for h in hashes)
        ambiguous = next(d for d, count in first_digits.items() if count > 1)
        result = _invoke(cli_runner, ["get", ambiguous], store_dir)
        assert result.exit_code == 1
        assert "Ambiguous prefix" in result.output


class TestDiff:
    def test_diff_commits(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def add(x: int, y: int) -> int:
            return x + y

        ref1 = client.submit(add, 1, 2, _cache=False)
        ref2 = client.submit(add, 3, 4, _cache=False)
        result = _invoke(cli_runner, ["diff", ref1.commit_hash, ref2.commit_hash], store_dir)
        assert result.exit_code == 0
        assert "args_changed" in result.output

    def test_diff_missing(self, cli_runner: CliRunner, store_dir: Path) -> None:
        result = _invoke(cli_runner, ["diff", "deadbeef", "deadbeef"], store_dir)
        assert result.exit_code == 1
        assert "not found" in result.output


class TestHistory:
    def test_history(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def val(x: int) -> int:
            return x

        ref = client.submit(val, 5, _cache=False)
        client.submit(val, 5, _cache=False)
        result = _invoke(cli_runner, ["history", ref.commit_hash], store_dir)
        assert result.exit_code == 0
        assert "completed" in result.output or "cached" in result.output

    def test_history_empty(self, cli_runner: CliRunner, store_dir: Path) -> None:
        result = _invoke(cli_runner, ["history", "deadbeef"], store_dir)
        assert result.exit_code == 0
        assert "No history found" in result.output

    def test_history_ambiguous_hash(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)
        hashes: list[str] = []
        for i in range(20):

            def make_val(idx: int = i) -> int:
                return idx

            ref = client.submit(make_val, _cache=False)
            hashes.append(ref.commit_hash)
        from collections import Counter

        first_digits = Counter(h[0] for h in hashes)
        ambiguous = next(d for d, count in first_digits.items() if count > 1)
        result = _invoke(cli_runner, ["history", ambiguous], store_dir)
        assert result.exit_code == 1
        assert "Ambiguous prefix" in result.output


class TestStats:
    def test_stats(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def val() -> int:
            return 42

        client.submit(val)
        result = _invoke(cli_runner, ["stats"], store_dir)
        assert result.exit_code == 0
        assert "total_commits" in result.output

    def test_stats_empty(self, cli_runner: CliRunner, store_dir: Path) -> None:
        result = _invoke(cli_runner, ["stats"], store_dir)
        assert result.exit_code == 0
        assert "total_commits" in result.output


class TestRm:
    def test_rm(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def val() -> int:
            return 1

        ref = client.submit(val, _cache=False)
        result = _invoke(cli_runner, ["rm", ref.commit_hash], store_dir)
        assert result.exit_code == 0
        assert "Deleted" in result.output

    def test_rm_missing(self, cli_runner: CliRunner, store_dir: Path) -> None:
        result = _invoke(cli_runner, ["rm", "deadbeef00000000000000000000000000000000"], store_dir)
        assert result.exit_code == 1
        assert "not found" in result.output


class TestGc:
    def test_gc(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def val() -> int:
            return 1

        client.submit(val, _cache=False)
        result = _invoke(cli_runner, ["gc", "--older-than", "0"], store_dir)
        assert result.exit_code == 0
        assert "Evicted" in result.output

    def test_gc_max_size(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def make_bytes() -> bytes:
            return b"x" * 500

        client.submit(make_bytes, _cache=False)
        result = _invoke(cli_runner, ["gc", "--max-size", "1B"], store_dir)
        assert result.exit_code == 0
        assert "Evicted" in result.output
        assert client.stats()["total_commits"] == 0


class TestClear:
    def test_clear(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def val() -> int:
            return 1

        client.submit(val, _cache=False)
        result = _invoke(cli_runner, ["clear"], store_dir)
        assert result.exit_code == 0
        assert "Cleared" in result.output


class TestList:
    def test_list_alias(self, cli_runner: CliRunner, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)

        def foo() -> int:
            return 1

        client.submit(foo)
        result = _invoke(cli_runner, ["list"], store_dir)
        assert result.exit_code == 0
        assert "foo" in result.output


class TestExportImport:
    def test_export(self, cli_runner: CliRunner, store_dir: Path, tmp_path: Path) -> None:
        client = Client(store_dir=store_dir)

        def add(x: int, y: int) -> int:
            return x + y

        client.submit(add, 1, 2)
        archive = tmp_path / "export.tar.gz"
        result = _invoke(cli_runner, ["export", str(archive)], store_dir)
        assert result.exit_code == 0
        assert "Exported" in result.output
        assert archive.exists()

    def test_import(self, cli_runner: CliRunner, store_dir: Path, tmp_path: Path) -> None:
        client = Client(store_dir=store_dir)

        def add(x: int, y: int) -> int:
            return x + y

        ref = client.submit(add, 1, 2)
        archive = tmp_path / "export.tar.gz"
        client.export(archive)

        store_dir2 = tmp_path / ".cashet2"
        store_dir2.mkdir()
        result = _invoke(cli_runner, ["import", str(archive)], store_dir2)
        assert result.exit_code == 0
        assert "Imported" in result.output

        client2 = Client(store_dir=store_dir2)
        assert client2.get(ref.commit_hash) == 3
