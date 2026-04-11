from __future__ import annotations

import json

import click
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from cashet.client import Client
from cashet.models import TaskStatus

console = Console()


def _client() -> Client:
    return Client()


def _parse_tags(tags: tuple[str, ...]) -> dict[str, str | None] | None:
    if not tags:
        return None
    result: dict[str, str | None] = {}
    for tag in tags:
        if "=" in tag:
            key, val = tag.split("=", 1)
            if not key:
                raise click.BadParameter(f"Invalid tag '{tag}': key cannot be empty")
            result[key] = val
        else:
            if not tag:
                raise click.BadParameter("Tag key cannot be empty")
            result[tag] = None
    return result


@click.group()
def main() -> None:
    """cashet — content-addressable compute cache with git semantics"""
    pass


@main.command("log")
@click.option("--func", "-f", default=None, help="Filter by function name")
@click.option("--limit", "-n", default=20, help="Max commits to show")
@click.option("--status", "-s", default=None, help="Filter by status")
@click.option("--tag", "-t", multiple=True, help="Filter by tag (key=value or key)")
def log_cmd(func: str | None, limit: int, status: str | None, tag: tuple[str, ...]) -> None:
    """Show commit history"""
    client = _client()
    status_enum: TaskStatus | None = None
    if status:
        try:
            status_enum = TaskStatus(status)
        except ValueError:
            valid = ", ".join(s.value for s in TaskStatus)
            console.print(f"[red]Invalid status '{status}'. Valid values: {valid}[/red]")
            raise SystemExit(1) from None
    commits = client.log(func_name=func, limit=limit, status=status_enum, tags=_parse_tags(tag))
    if not commits:
        console.print("[dim]No commits found.[/dim]")
        return
    table = Table(title="Cashet Log")
    table.add_column("Hash", style="cyan", width=12)
    table.add_column("Function", style="green")
    table.add_column("Status", style="yellow")
    table.add_column("Size", style="blue", justify="right")
    table.add_column("Timestamp", style="dim")
    for c in commits:
        size_str = f"{c.output_ref.size}b" if c.output_ref else "-"
        status_style = {
            "completed": "green",
            "cached": "cyan",
            "failed": "red",
            "running": "yellow",
            "pending": "dim",
        }.get(c.status.value, "white")
        table.add_row(
            c.short_hash(),
            c.task_def.func_name,
            f"[{status_style}]{c.status.value}[/{status_style}]",
            size_str,
            c.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        )
    console.print(table)


@main.command("show")
@click.argument("hash")
def show_cmd(hash: str) -> None:
    """Show details of a specific commit"""
    client = _client()
    try:
        commit = client.show(hash)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None
    if commit is None:
        console.print(f"[red]Commit {hash} not found.[/red]")
        return
    out_hash = commit.output_ref.hash[:12] if commit.output_ref else "none"
    out_size = commit.output_ref.size if commit.output_ref else 0
    parent = commit.parent_hash[:12] if commit.parent_hash else "none"
    tags_str = ", ".join(f"{k}={v}" for k, v in commit.tags.items()) if commit.tags else "none"
    info = Panel(
        f"[cyan]Hash:[/cyan]     {commit.hash}\n"
        f"[green]Function:[/green] {commit.task_def.func_name}\n"
        f"[yellow]Status:[/yellow]   {commit.status.value}\n"
        f"[blue]Output:[/blue]    {out_hash} ({out_size}b)\n"
        f"[dim]Created:[/dim]   {commit.created_at.isoformat()}\n"
        f"[dim]Parent:[/dim]    {parent}\n"
        f"[dim]Cache:[/dim]     {commit.task_def.cache}\n"
        f"[dim]Tags:[/dim]     {tags_str}",
        title=f"Commit {commit.short_hash()}",
    )
    console.print(info)
    if commit.task_def.func_source:
        console.print(Panel(Syntax(commit.task_def.func_source, "python"), title="Source"))
    if commit.task_def.args_snapshot:
        console.print(
            Panel(
                commit.task_def.args_snapshot.decode("utf-8", errors="replace"),
                title="Args",
            )
        )
    if commit.error:
        console.print(Panel(commit.error, title="Error", style="red"))


@main.command("get")
@click.argument("hash")
@click.option("--output", "-o", default=None, help="Write output to file")
def get_cmd(hash: str, output: str | None) -> None:
    """Retrieve a stored result"""
    import pathlib

    client = _client()
    try:
        commit = client.show(hash)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None
    if commit is None or commit.output_ref is None:
        console.print(f"[red]No result for {hash}[/red]")
        return
    ref = client.store.get_blob(commit.output_ref)
    if output:
        pathlib.Path(output).write_bytes(ref)
        console.print(f"Written {len(ref)} bytes to {output}")
    else:
        console.print(f"[dim]{len(ref)} bytes — use -o to write to file[/dim]")


@main.command("diff")
@click.argument("hash_a")
@click.argument("hash_b")
def diff_cmd(hash_a: str, hash_b: str) -> None:
    """Compare two commits"""
    client = _client()
    try:
        d = client.diff(hash_a, hash_b)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None
    console.print(Panel(json.dumps(d, indent=2, default=str), title="Diff"))


@main.command("list")
@click.option("--func", "-f", default=None, help="Filter by function name")
@click.option("--tag", "-t", multiple=True, help="Filter by tag (key=value or key)")
def list_cmd(func: str | None, tag: tuple[str, ...]) -> None:
    """List all commits (alias for log)"""
    client = _client()
    commits = client.log(func_name=func, tags=_parse_tags(tag))
    for c in commits:
        marker = "\u2713" if c.status.value in ("completed", "cached") else "\u2717"
        ts = c.created_at.strftime("%Y-%m-%d %H:%M")
        console.print(f"  {marker} [cyan]{c.short_hash()}[/cyan] {c.task_def.func_name} {ts}")


@main.command("history")
@click.argument("hash")
def history_cmd(hash: str) -> None:
    """Show the history of a commit (same function+args over time)"""
    client = _client()
    try:
        commits = client.history(hash)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None
    if not commits:
        console.print("[dim]No history found.[/dim]")
        return
    for i, c in enumerate(commits):
        prefix = "\u2192" if i == len(commits) - 1 else "\u2502"
        marker = "*" if c.hash.startswith(hash) else " "
        ts = c.created_at.strftime("%Y-%m-%d %H:%M")
        console.print(
            f"  {prefix} {marker} [cyan]{c.short_hash()}[/cyan] [dim]{ts}[/dim] {c.status.value}"
        )


@main.command("stats")
def stats_cmd() -> None:
    """Show storage statistics"""
    client = _client()
    s = client.stats()
    table = Table(title="Cashet Stats")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green", justify="right")
    for k, v in s.items():
        table.add_row(k, str(v))
    console.print(table)


@main.command("rm")
@click.argument("hash")
def rm_cmd(hash: str) -> None:
    """Delete a specific commit and orphaned blobs"""
    client = _client()
    try:
        deleted = client.rm(hash)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None
    if deleted:
        console.print(f"[green]Deleted commit {hash[:12]}.[/green]")
    else:
        console.print(f"[red]Commit {hash[:12]} not found.[/red]")
        raise SystemExit(1)


@main.command("gc")
@click.option("--older-than", "-d", default=30, help="Evict entries older than N days")
def gc_cmd(older_than: int) -> None:
    """Evict old cache entries and orphaned blobs"""
    from datetime import timedelta

    client = _client()
    deleted = client.gc(timedelta(days=older_than))
    console.print(f"[green]Evicted {deleted} commit(s) older than {older_than} day(s).[/green]")


if __name__ == "__main__":
    main()
