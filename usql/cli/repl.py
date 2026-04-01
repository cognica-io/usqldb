#
# usql -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

r"""usql -- interactive SQL shell with PostgreSQL 17-compatible catalogs.

Usage:
    usql                        Start with an in-memory database
    usql --db mydata.db         Start with persistent SQLite storage
    usql script.sql             Execute a SQL script then enter REPL
    usql --db mydata.db s.sql   Persistent + script
    usql -c "SELECT 1"          Execute a command string and exit

Special commands (backslash):
    \d [NAME]       Describe table/view or list all relations
    \dt             List tables
    \di             List indexes
    \dv             List views
    \x              Toggle expanded display
    \timing         Toggle query timing
    \?              Show all commands
    \q              Quit
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import FileHistory
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style
from pygments.lexers.sql import SqlLexer

import usql
from usql.cli.commands import CommandHandler
from usql.cli.completer import USQLCompleter
from usql.cli.formatter import Formatter
from usql.core.engine import USQLEngine

_STYLE = Style.from_dict(
    {
        "prompt": "ansicyan bold",
        "prompt.continuation": "ansibrightblack",
        "bottom-toolbar": "bg:ansibrightblack ansiwhite",
    }
)


class USQLShell:
    """Interactive SQL shell backed by a USQLEngine."""

    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path
        self._engine = USQLEngine(db_path=db_path)

        self._formatter = Formatter()
        self._commands = CommandHandler(
            engine=self._engine,
            formatter=self._formatter,
            output_fn=print,
        )
        self._commands.db_path = db_path
        self._commands.execute_file_fn = self.run_file

        self._completer = USQLCompleter(self._engine)
        self._session: PromptSession | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_file(self, path: str) -> None:
        """Execute every statement in a SQL script file."""
        with open(path) as f:
            text = f.read()
        self._execute_text(text)

    def repl(self) -> None:
        """Enter the read-eval-print loop."""
        session = self._ensure_session()
        self._print_banner()
        buf = ""

        while True:
            try:
                if not buf:
                    prompt_text = HTML("<prompt>uqa=&gt; </prompt>")
                else:
                    prompt_text = HTML(
                        "<prompt.continuation>uqa-&gt; </prompt.continuation>"
                    )
                line = session.prompt(
                    prompt_text,
                    bottom_toolbar=self._toolbar,
                )
            except KeyboardInterrupt:
                if buf:
                    buf = ""
                    print()
                    continue
                print()
                continue
            except EOFError:
                print()
                break

            stripped = line.strip()

            # Empty line with no buffer: skip
            if not stripped and not buf:
                continue

            # Backslash commands only at the start (not in multi-line buffer)
            if not buf and stripped.startswith("\\"):
                try:
                    should_quit = self._commands.handle(stripped)
                except Exception as exc:
                    print(f"ERROR: {exc}")
                    continue
                if should_quit:
                    break
                continue

            buf += line + "\n"

            # Semicolon terminates the statement
            if ";" not in buf:
                continue

            self._execute_text(buf)
            buf = ""

    # ------------------------------------------------------------------
    # Statement execution
    # ------------------------------------------------------------------

    def _execute_text(self, text: str) -> None:
        """Split on semicolons and execute each statement."""
        for raw in text.split(";"):
            stmt = raw.strip()
            if not stmt:
                continue
            # Skip pure comment blocks
            if all(
                ln.strip().startswith("--") or not ln.strip()
                for ln in stmt.splitlines()
            ):
                continue
            self._execute_one(stmt)

    def _execute_one(self, stmt: str) -> None:
        t0 = time.perf_counter()
        try:
            result = self._engine.sql(stmt)
        except Exception as exc:
            print(f"ERROR: {exc}")
            return
        elapsed = (time.perf_counter() - t0) * 1000.0

        self._print_result(result)
        if self._commands.show_timing:
            self._print(f"Time: {elapsed:.3f} ms")

    def _print_result(self, result: object) -> None:
        """Format and display a SQL result."""
        from uqa.sql.compiler import SQLResult

        if not isinstance(result, SQLResult):
            return
        if not result.columns and not result.rows:
            return
        text = self._formatter.format_result(result)
        if text:
            self._print(text)

    def _print(self, text: str) -> None:
        """Write to the current output destination."""
        if self._commands.output_file is not None:
            with open(self._commands.output_file, "a") as f:
                f.write(text + "\n")
        else:
            print(text)

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    @staticmethod
    def _history_path() -> str:
        history_dir = Path(os.path.expanduser("~/.cognica/usql"))
        history_dir.mkdir(parents=True, exist_ok=True)
        return str(history_dir / ".usql_history")

    def _ensure_session(self) -> PromptSession:
        if self._session is None:
            self._session = PromptSession(
                history=FileHistory(self._history_path()),
                auto_suggest=AutoSuggestFromHistory(),
                lexer=PygmentsLexer(SqlLexer),
                completer=self._completer,
                style=_STYLE,
                multiline=False,
                complete_while_typing=True,
            )
        return self._session

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _toolbar(self) -> str:
        nt = len(self._engine._tables)
        nv = len(self._engine._views)
        nf = len(self._engine._foreign_tables)
        timing = "on" if self._commands.show_timing else "off"
        expanded = "on" if self._formatter.expanded else "off"
        db = self._db_path or ":memory:"

        parts = [f"db: {db}", f"tables: {nt}"]
        if nv:
            parts.append(f"views: {nv}")
        if nf:
            parts.append(f"foreign: {nf}")
        parts.extend([f"timing: {timing}", f"expanded: {expanded}"])
        if self._commands.output_file:
            parts.append(f"output: {self._commands.output_file}")
        parts.append("\\? for help")
        return " usql | " + " | ".join(parts) + " "

    def _print_banner(self) -> None:
        db = self._db_path or ":memory:"
        print(f"usql {usql.__version__} (PostgreSQL 17.0 compatible)")
        print(f"Database: {db}")
        print('Type "\\?" for help.')
        print()


# ======================================================================
# Entry point
# ======================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="usql -- interactive SQL shell (PostgreSQL 17 compatible)"
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="SQLite database file for persistent storage",
    )
    parser.add_argument(
        "-c",
        metavar="COMMAND",
        default=None,
        help="Execute a single SQL command string and exit",
    )
    parser.add_argument(
        "scripts",
        nargs="*",
        metavar="script.sql",
        help="SQL script files to execute before entering REPL",
    )
    args = parser.parse_args()

    shell = USQLShell(db_path=args.db)

    # -c: execute command and exit
    if args.c is not None:
        try:
            shell._execute_text(args.c)
        finally:
            shell._engine.close()
        return

    for path in args.scripts:
        try:
            shell.run_file(path)
        except FileNotFoundError:
            print(f"File not found: {path}", file=sys.stderr)
            sys.exit(1)

    try:
        if sys.stdin.isatty():
            shell.repl()
        elif not args.scripts:
            shell.repl()
    finally:
        shell._engine.close()


if __name__ == "__main__":
    main()
