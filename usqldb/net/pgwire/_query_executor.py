#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Bridge between the pgwire protocol layer and USQLEngine.

The :class:`QueryExecutor` translates wire-protocol requests into
``USQLEngine.sql()`` calls, builds :class:`ColumnDescription` metadata,
generates PostgreSQL ``CommandComplete`` tags, and intercepts statements
that UQA does not handle natively (``SET``, ``SHOW``, ``RESET``,
``DISCARD``, ``BEGIN``, ``COMMIT``, ``ROLLBACK``).
"""

from __future__ import annotations

import asyncio
import re
from typing import TYPE_CHECKING, Any

from pglast import parse_sql, split

from usqldb.net.pgwire._constants import FORMAT_TEXT
from usqldb.net.pgwire._errors import (
    QueryCanceled,
    map_engine_exception,
)
from usqldb.net.pgwire._messages import ColumnDescription
from usqldb.net.pgwire._type_codec import TypeCodec
from usqldb.pg_compat.oid import TYPE_LENGTHS, type_oid

if TYPE_CHECKING:
    from usqldb.core.engine import USQLEngine

# Internal columns that should not appear in wire-protocol results.
_INTERNAL_COLUMNS = frozenset({"_doc_id", "_score"})

# Regex for SET statements: SET name = value / SET name TO value
_SET_RE = re.compile(
    r"^\s*SET\s+(?:SESSION\s+|LOCAL\s+)?"
    r"(\w+(?:\.\w+)?)\s*(?:=|TO)\s*(.+?)\s*;?\s*$",
    re.IGNORECASE,
)

# Regex for SHOW statements: SHOW name
_SHOW_RE = re.compile(r"^\s*SHOW\s+(\w+(?:\.\w+)?)\s*;?\s*$", re.IGNORECASE)

# Regex for RESET: RESET name / RESET ALL
_RESET_RE = re.compile(r"^\s*RESET\s+(ALL|\w+(?:\.\w+)?)\s*;?\s*$", re.IGNORECASE)

# Regex for DISCARD: DISCARD ALL / DISCARD PLANS / etc.
_DISCARD_RE = re.compile(r"^\s*DISCARD\s+(\w+)\s*;?\s*$", re.IGNORECASE)

# Transaction commands
_TX_BEGIN_RE = re.compile(r"^\s*(BEGIN|START\s+TRANSACTION)\b", re.IGNORECASE)
_TX_COMMIT_RE = re.compile(r"^\s*(COMMIT|END)\b", re.IGNORECASE)
_TX_ROLLBACK_RE = re.compile(r"^\s*ROLLBACK\b", re.IGNORECASE)

# DEALLOCATE for closing prepared statements via SQL
_DEALLOCATE_RE = re.compile(
    r"^\s*DEALLOCATE\s+(?:PREPARE\s+)?(?:ALL|(\w+))\s*;?\s*$",
    re.IGNORECASE,
)

# LISTEN / UNLISTEN / NOTIFY
_LISTEN_RE = re.compile(r"^\s*LISTEN\s+", re.IGNORECASE)
_UNLISTEN_RE = re.compile(r"^\s*UNLISTEN\s+", re.IGNORECASE)
_NOTIFY_RE = re.compile(r"^\s*NOTIFY\s+", re.IGNORECASE)

# Statement type names from pglast AST
_STMT_TYPE_TO_TAG: dict[str, str] = {
    "SelectStmt": "SELECT",
    "InsertStmt": "INSERT",
    "UpdateStmt": "UPDATE",
    "DeleteStmt": "DELETE",
    "CreateStmt": "CREATE TABLE",
    "CreateTableAsStmt": "SELECT",
    "DropStmt": "DROP TABLE",
    "AlterTableStmt": "ALTER TABLE",
    "IndexStmt": "CREATE INDEX",
    "ViewStmt": "CREATE VIEW",
    "CreateSeqStmt": "CREATE SEQUENCE",
    "AlterSeqStmt": "ALTER SEQUENCE",
    "TruncateStmt": "TRUNCATE TABLE",
    "CreateSchemaStmt": "CREATE SCHEMA",
    "CreateForeignServerStmt": "CREATE SERVER",
    "CreateForeignTableStmt": "CREATE FOREIGN TABLE",
    "CreateFdwStmt": "CREATE FOREIGN DATA WRAPPER",
    "ExplainStmt": "EXPLAIN",
    "VariableSetStmt": "SET",
    "VariableShowStmt": "SHOW",
    "TransactionStmt": "BEGIN",
    "CopyStmt": "COPY",
}


class QueryResult:
    """Result of a query execution with pgwire metadata."""

    __slots__ = (
        "columns",
        "command_tag",
        "is_select",
        "rows",
    )

    def __init__(
        self,
        columns: list[ColumnDescription],
        rows: list[dict[str, Any]],
        command_tag: str,
        *,
        is_select: bool = False,
    ) -> None:
        self.columns = columns
        self.rows = rows
        self.command_tag = command_tag
        self.is_select = is_select


class QueryExecutor:
    """Executes SQL queries via USQLEngine and translates results."""

    def __init__(self, engine: USQLEngine) -> None:
        self._engine = engine
        self._session_params: dict[str, str] = {}

    @property
    def session_params(self) -> dict[str, str]:
        """Current session parameters (SET values)."""
        return self._session_params

    # ==================================================================
    # Main execution entry point
    # ==================================================================

    async def execute(
        self,
        query: str,
        params: list[Any] | None = None,
    ) -> QueryResult:
        """Execute a single SQL statement and return the pgwire result."""
        # Try intercepting connection-level commands first.
        intercepted = self._try_intercept(query)
        if intercepted is not None:
            return intercepted

        # Execute via engine in a thread to avoid blocking the event loop.
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(None, self._engine.sql, query, params)
        except Exception as exc:
            from uqa.cancel import QueryCancelled

            if isinstance(exc, QueryCancelled):
                raise QueryCanceled(str(exc)) from exc
            raise map_engine_exception(exc) from exc

        return self._build_result(query, result)

    def execute_sync(
        self,
        query: str,
        params: list[Any] | None = None,
    ) -> QueryResult:
        """Execute synchronously (for use inside run_in_executor)."""
        intercepted = self._try_intercept(query)
        if intercepted is not None:
            return intercepted

        try:
            result = self._engine.sql(query, params)
        except Exception as exc:
            from uqa.cancel import QueryCancelled

            if isinstance(exc, QueryCancelled):
                raise QueryCanceled(str(exc)) from exc
            raise map_engine_exception(exc) from exc

        return self._build_result(query, result)

    # ==================================================================
    # Multi-statement splitting
    # ==================================================================

    @staticmethod
    def split_statements(sql: str) -> list[str]:
        """Split a SQL string into individual statements using pglast."""
        stripped = sql.strip()
        if not stripped:
            return []
        try:
            return list(split(stripped))
        except Exception:
            # If pglast cannot parse, return the whole string.
            return [stripped]

    # ==================================================================
    # Command interception
    # ==================================================================

    def _try_intercept(self, query: str) -> QueryResult | None:
        """Intercept commands that UQA does not support natively."""
        m = _SET_RE.match(query)
        if m:
            return self._handle_set(m.group(1), m.group(2))

        m = _SHOW_RE.match(query)
        if m:
            return self._handle_show(m.group(1))

        m = _RESET_RE.match(query)
        if m:
            return self._handle_reset(m.group(1))

        m = _DISCARD_RE.match(query)
        if m:
            return self._handle_discard(m.group(1))

        if _TX_BEGIN_RE.match(query):
            return self._handle_transaction("BEGIN")
        if _TX_COMMIT_RE.match(query):
            return self._handle_transaction("COMMIT")
        if _TX_ROLLBACK_RE.match(query):
            return self._handle_transaction("ROLLBACK")

        if _DEALLOCATE_RE.match(query):
            return QueryResult([], [], "DEALLOCATE")

        if _LISTEN_RE.match(query):
            return QueryResult([], [], "LISTEN")
        if _UNLISTEN_RE.match(query):
            return QueryResult([], [], "UNLISTEN")
        if _NOTIFY_RE.match(query):
            return QueryResult([], [], "NOTIFY")

        return None

    def _handle_set(self, name: str, value: str) -> QueryResult:
        """Handle SET parameter = value."""
        # Strip quotes from value.
        cleaned = value.strip().strip("'\"")
        self._session_params[name.lower()] = cleaned
        return QueryResult([], [], "SET")

    def _handle_show(self, name: str) -> QueryResult:
        """Handle SHOW parameter."""
        from usqldb.net.pgwire._constants import DEFAULT_SERVER_PARAMS

        key = name.lower()
        value = self._session_params.get(key)
        if value is None:
            value = DEFAULT_SERVER_PARAMS.get(key, "")

        # SHOW returns a single row with the parameter name as column.
        col = ColumnDescription(
            name=key,
            table_oid=0,
            column_number=0,
            type_oid=25,  # text
            type_size=-1,
            type_modifier=-1,
            format_code=FORMAT_TEXT,
        )
        return QueryResult(
            columns=[col],
            rows=[{key: value}],
            command_tag="SHOW",
            is_select=True,
        )

    def _handle_reset(self, name: str) -> QueryResult:
        """Handle RESET parameter / RESET ALL."""
        if name.upper() == "ALL":
            self._session_params.clear()
        else:
            self._session_params.pop(name.lower(), None)
        return QueryResult([], [], "RESET")

    def _handle_discard(self, what: str) -> QueryResult:
        """Handle DISCARD ALL / DISCARD PLANS / etc."""
        if what.upper() == "ALL":
            self._session_params.clear()
        return QueryResult([], [], "DISCARD ALL")

    def _handle_transaction(self, cmd: str) -> QueryResult:
        """Handle BEGIN/COMMIT/ROLLBACK as no-ops for in-memory engines."""
        return QueryResult([], [], cmd)

    # ==================================================================
    # Result building
    # ==================================================================

    def _build_result(self, query: str, result: Any) -> QueryResult:
        """Convert a USQLEngine result to a QueryResult."""
        columns_raw: list[str] = result.columns if result.columns else []
        rows_raw: list[dict[str, Any]] = result.rows if result.rows else []

        # Detect command type for the tag.
        cmd_type = self._detect_command_type(query)

        # For DML results, build the appropriate tag.
        if cmd_type == "INSERT":
            count = rows_raw[0].get("inserted", 0) if rows_raw else 0
            return QueryResult([], [], f"INSERT 0 {count}")

        if cmd_type == "UPDATE":
            count = rows_raw[0].get("updated", 0) if rows_raw else 0
            return QueryResult([], [], f"UPDATE {count}")

        if cmd_type == "DELETE":
            count = rows_raw[0].get("deleted", 0) if rows_raw else 0
            return QueryResult([], [], f"DELETE {count}")

        # DDL: no rows
        if not columns_raw:
            return QueryResult([], [], cmd_type)

        # SELECT: filter internal columns and build descriptions.
        visible_columns = [c for c in columns_raw if c not in _INTERNAL_COLUMNS]

        col_descs = self._build_column_descriptions(visible_columns, rows_raw)

        # Filter rows to only visible columns.
        filtered_rows = [
            {k: v for k, v in row.items() if k not in _INTERNAL_COLUMNS}
            for row in rows_raw
        ]

        tag = f"SELECT {len(filtered_rows)}"
        return QueryResult(
            columns=col_descs,
            rows=filtered_rows,
            command_tag=tag,
            is_select=True,
        )

    def _build_column_descriptions(
        self,
        columns: list[str],
        rows: list[dict[str, Any]],
    ) -> list[ColumnDescription]:
        """Build ColumnDescription list for visible columns."""
        descriptions: list[ColumnDescription] = []

        for col_name in columns:
            oid = 25  # default: text
            table_oid = 0
            col_number = 0
            type_size = -1
            type_mod = -1

            # Try to resolve from table metadata.
            for _tname, table in self._engine._tables.items():
                if col_name in table.columns:
                    col_def = table.columns[col_name]
                    oid = type_oid(col_def.type_name)
                    type_size = TYPE_LENGTHS.get(oid, -1)
                    col_keys = list(table.columns.keys())
                    col_number = col_keys.index(col_name) + 1
                    break

            # Fall back to value-based inference.
            if oid == 25 and rows:
                first_val = None
                for row in rows:
                    val = row.get(col_name)
                    if val is not None:
                        first_val = val
                        break
                if first_val is not None:
                    oid = TypeCodec.infer_type_oid(first_val)
                    type_size = TYPE_LENGTHS.get(oid, -1)

            descriptions.append(
                ColumnDescription(
                    name=col_name,
                    table_oid=table_oid,
                    column_number=col_number,
                    type_oid=oid,
                    type_size=type_size,
                    type_modifier=type_mod,
                    format_code=FORMAT_TEXT,
                )
            )

        return descriptions

    def _detect_command_type(self, query: str) -> str:
        """Detect the SQL command type for CommandComplete tags."""
        try:
            stmts = parse_sql(query)
            if stmts:
                stmt_type = type(stmts[0].stmt).__name__
                return _STMT_TYPE_TO_TAG.get(stmt_type, stmt_type.upper())
        except Exception:
            pass

        # Fallback: first word.
        first_word = query.strip().split()[0].upper() if query.strip() else ""
        if first_word == "CREATE":
            words = query.strip().split()
            if len(words) >= 2:
                return f"CREATE {words[1].upper()}"
        if first_word == "DROP":
            words = query.strip().split()
            if len(words) >= 2:
                return f"DROP {words[1].upper()}"
        return first_word
