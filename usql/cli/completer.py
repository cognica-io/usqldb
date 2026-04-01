#
# usql -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Context-aware SQL completer for the usql interactive shell.

Provides tab-completion for SQL keywords, table/view/column names,
schema-qualified names, and backslash commands.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from prompt_toolkit.completion import Completer, Completion

if TYPE_CHECKING:
    from collections.abc import Iterable

    from prompt_toolkit.document import Document

    from usql.core.engine import USQLEngine

# SQL keywords (uppercase) -- covers DDL, DML, DQL, and UQA extensions
_SQL_KEYWORDS: list[str] = [
    # DDL
    "CREATE",
    "TABLE",
    "DROP",
    "IF",
    "EXISTS",
    "PRIMARY",
    "KEY",
    "NOT",
    "NULL",
    "DEFAULT",
    "SERIAL",
    "BIGSERIAL",
    "ALTER",
    "ADD",
    "COLUMN",
    "RENAME",
    "TO",
    "SET",
    "TRUNCATE",
    "UNIQUE",
    "CHECK",
    "CONSTRAINT",
    "REFERENCES",
    "FOREIGN",
    "CASCADE",
    "RESTRICT",
    "TEMPORARY",
    "TEMP",
    "VIEW",
    "INDEX",
    "SEQUENCE",
    "USING",
    # Types
    "INTEGER",
    "INT",
    "BIGINT",
    "SMALLINT",
    "TEXT",
    "VARCHAR",
    "REAL",
    "FLOAT",
    "DOUBLE",
    "PRECISION",
    "NUMERIC",
    "DECIMAL",
    "BOOLEAN",
    "BOOL",
    "CHAR",
    "CHARACTER",
    "JSON",
    "JSONB",
    "UUID",
    "BYTEA",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "INTERVAL",
    "POINT",
    "VECTOR",
    # DML
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "DELETE",
    "RETURNING",
    "ON",
    "CONFLICT",
    "DO",
    "NOTHING",
    "EXCLUDED",
    # DQL
    "SELECT",
    "FROM",
    "WHERE",
    "AND",
    "OR",
    "IN",
    "BETWEEN",
    "ORDER",
    "BY",
    "ASC",
    "DESC",
    "LIMIT",
    "OFFSET",
    "AS",
    "DISTINCT",
    "GROUP",
    "HAVING",
    "LIKE",
    "ILIKE",
    "IS",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "CAST",
    "COALESCE",
    "NULLIF",
    "UNION",
    "ALL",
    "EXCEPT",
    "INTERSECT",
    "TRUE",
    "FALSE",
    # Joins
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "CROSS",
    "OUTER",
    # Subqueries / CTE
    "WITH",
    "RECURSIVE",
    # Aggregates
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "ARRAY_AGG",
    "STRING_AGG",
    "BOOL_AND",
    "BOOL_OR",
    "FILTER",
    # Window functions
    "OVER",
    "PARTITION",
    "WINDOW",
    "ROWS",
    "RANGE",
    "UNBOUNDED",
    "PRECEDING",
    "FOLLOWING",
    "CURRENT",
    "ROW",
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "NTILE",
    "LAG",
    "LEAD",
    "FIRST_VALUE",
    "LAST_VALUE",
    "NTH_VALUE",
    "PERCENT_RANK",
    "CUME_DIST",
    # FDW
    "SERVER",
    "DATA",
    "WRAPPER",
    "OPTIONS",
    "IMPORT",
    # Transaction
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    # Utility
    "EXPLAIN",
    "ANALYZE",
    "PREPARE",
    "EXECUTE",
    "DEALLOCATE",
    "GENERATE_SERIES",
    # UQA extensions
    "text_match",
    "bayesian_match",
    "knn_match",
    "traverse",
    "rpq",
    "traverse_match",
    "fuse_log_odds",
    "fuse_prob_and",
    "fuse_prob_or",
    "fuse_prob_not",
    "fuse_attention",
    "fuse_learned",
    "spatial_within",
    "multi_field_match",
    "bayesian_knn_match",
    "deep_fusion",
    "deep_learn",
    "deep_predict",
    # Schema names
    "information_schema",
    "pg_catalog",
]

_BACKSLASH_COMMANDS: list[tuple[str, str]] = [
    ("\\d", "Describe table/view/index or list relations"),
    ("\\dt", "List tables"),
    ("\\di", "List indexes"),
    ("\\dv", "List views"),
    ("\\ds", "List sequences"),
    ("\\df", "List functions"),
    ("\\dn", "List schemas"),
    ("\\du", "List roles"),
    ("\\l", "List databases"),
    ("\\det", "List foreign tables"),
    ("\\des", "List foreign servers"),
    ("\\dew", "List foreign data wrappers"),
    ("\\dG", "List named graphs"),
    ("\\x", "Toggle expanded display"),
    ("\\timing", "Toggle timing"),
    ("\\o", "Redirect output to file"),
    ("\\i", "Execute commands from file"),
    ("\\e", "Edit query in external editor"),
    ("\\conninfo", "Display connection info"),
    ("\\encoding", "Show client encoding"),
    ("\\!", "Execute shell command"),
    ("\\?", "Show help"),
    ("\\q", "Quit"),
]

# Keywords that typically precede a table name
_TABLE_PRECEDING = frozenset(
    {
        "FROM",
        "JOIN",
        "INTO",
        "TABLE",
        "ANALYZE",
        "UPDATE",
        "DELETE",
        "INNER",
        "LEFT",
        "RIGHT",
        "FULL",
        "CROSS",
    }
)


class USQLCompleter(Completer):
    """Context-aware SQL completer with dynamic table/column names."""

    def __init__(self, engine: USQLEngine) -> None:
        self._engine = engine

    def get_completions(
        self, document: Document, complete_event: object
    ) -> Iterable[Completion]:
        text = document.text_before_cursor

        # -- Backslash commands -------------------------------------------
        if text.lstrip().startswith("\\"):
            prefix = text.lstrip()
            for cmd, desc in _BACKSLASH_COMMANDS:
                if cmd.startswith(prefix):
                    yield Completion(
                        cmd,
                        start_position=-len(prefix),
                        display_meta=desc,
                    )
            # After backslash command, complete table names
            parts = prefix.split(None, 1)
            if len(parts) == 2:
                word = parts[1]
                yield from self._table_completions(word)
            return

        word = document.get_word_before_cursor()
        if not word:
            return

        upper = word.upper()

        # Detect if previous keyword expects a table name
        before = text[: -len(word)].upper().rstrip()
        after_table_kw = any(before.endswith(kw) for kw in _TABLE_PRECEDING)

        candidates: list[tuple[str, str]] = []

        # SQL keywords
        for kw in _SQL_KEYWORDS:
            if kw.upper().startswith(upper):
                candidates.append((kw, "keyword"))

        # Table names (regular + foreign)
        for name in self._engine._tables:
            if name.upper().startswith(upper):
                candidates.append((name, "table"))
        for name in self._engine._foreign_tables:
            if name.upper().startswith(upper):
                candidates.append((name, "foreign table"))

        # View names
        for name in self._engine._views:
            if name.upper().startswith(upper):
                candidates.append((name, "view"))

        # Column names (only when not after a table keyword)
        if not after_table_kw:
            seen: set[str] = set()
            for table in self._engine._tables.values():
                for col_name in table.columns:
                    if col_name not in seen and col_name.upper().startswith(upper):
                        seen.add(col_name)
                        candidates.append((col_name, "column"))
            for ftable in self._engine._foreign_tables.values():
                for col_name in ftable.columns:
                    if col_name not in seen and col_name.upper().startswith(upper):
                        seen.add(col_name)
                        candidates.append((col_name, "column"))

        # Sort: tables first after FROM/JOIN, keywords first otherwise
        def sort_key(item: tuple[str, str]) -> tuple[int, str]:
            text_val, kind = item
            if after_table_kw:
                order = {
                    "table": 0,
                    "view": 1,
                    "foreign table": 2,
                    "keyword": 3,
                    "column": 4,
                }
            else:
                order = {
                    "keyword": 0,
                    "column": 1,
                    "table": 2,
                    "view": 3,
                    "foreign table": 4,
                }
            return (order.get(kind, 9), text_val.lower())

        candidates.sort(key=sort_key)

        for text_val, kind in candidates:
            yield Completion(
                text_val,
                start_position=-len(word),
                display_meta=kind,
            )

    def _table_completions(self, prefix: str) -> Iterable[Completion]:
        """Yield table/view name completions for a prefix."""
        upper = prefix.upper()
        for name in sorted(self._engine._tables):
            if name.upper().startswith(upper):
                yield Completion(
                    name, start_position=-len(prefix), display_meta="table"
                )
        for name in sorted(self._engine._views):
            if name.upper().startswith(upper):
                yield Completion(name, start_position=-len(prefix), display_meta="view")
        for name in sorted(self._engine._foreign_tables):
            if name.upper().startswith(upper):
                yield Completion(
                    name, start_position=-len(prefix), display_meta="foreign table"
                )
