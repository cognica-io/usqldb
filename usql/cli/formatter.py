#
# usql -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Output formatting for the usql interactive shell.

Produces psql-compatible tabular and expanded output.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from uqa.sql.compiler import SQLResult


class Formatter:
    """Stateful output formatter matching psql display conventions.

    Supports two modes:
        - **aligned** (default): columnar table with ``|`` separators
        - **expanded** (``\\x``): vertical, one column per line
    """

    def __init__(self) -> None:
        self.expanded: bool = False
        self.null_display: str = ""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def format_result(
        self,
        result: SQLResult,
        title: str | None = None,
    ) -> str:
        """Format a SQLResult for terminal display."""
        columns = result.columns
        rows = result.rows
        if not columns and not rows:
            return ""
        if self.expanded:
            return self._format_expanded(columns, rows)
        return self._format_aligned(columns, rows, title)

    def format_rows(
        self,
        columns: list[str],
        rows: list[dict[str, Any]],
        title: str | None = None,
    ) -> str:
        """Format raw column/row data for terminal display."""
        if self.expanded:
            return self._format_expanded(columns, rows)
        return self._format_aligned(columns, rows, title)

    # ------------------------------------------------------------------
    # Aligned (tabular) format
    # ------------------------------------------------------------------

    def _format_aligned(
        self,
        columns: list[str],
        rows: list[dict[str, Any]],
        title: str | None = None,
    ) -> str:
        if not rows and not columns:
            return "(0 rows)"

        # Stringify all values
        widths: dict[str, int] = {col: len(col) for col in columns}
        str_rows: list[dict[str, str]] = []
        for row in rows:
            sr: dict[str, str] = {}
            for col in columns:
                s = self._format_value(row.get(col))
                sr[col] = s
                widths[col] = max(widths[col], len(s))
            str_rows.append(sr)

        parts: list[str] = []

        # Optional title centered above the table
        if title:
            table_width = sum(widths.values()) + 3 * (len(columns) - 1) + 2
            parts.append(title.center(table_width))

        # Header
        header = " " + " | ".join(col.center(widths[col]) for col in columns)
        parts.append(header)

        # Separator
        sep = "-" + "-+-".join("-" * widths[col] for col in columns) + "-"
        parts.append(sep)

        # Data rows
        for sr in str_rows:
            line = " " + " | ".join(sr[col].ljust(widths[col]) for col in columns)
            parts.append(line)

        # Footer
        n = len(str_rows)
        if n == 0:
            parts.append("(0 rows)")
        elif n == 1:
            parts.append("(1 row)")
        else:
            parts.append(f"({n} rows)")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Expanded (vertical) format
    # ------------------------------------------------------------------

    def _format_expanded(
        self,
        columns: list[str],
        rows: list[dict[str, Any]],
    ) -> str:
        if not rows:
            return "(0 rows)"

        col_width = max(len(c) for c in columns)
        parts: list[str] = []

        for i, row in enumerate(rows):
            # Record header
            label = f"-[ RECORD {i + 1} ]"
            padding = max(0, col_width + 3 - len(label))
            parts.append(label + "-" * padding)
            # Column values
            for col in columns:
                val = self._format_value(row.get(col))
                parts.append(f"{col:<{col_width}} | {val}")

        n = len(rows)
        if n == 1:
            parts.append("(1 row)")
        else:
            parts.append(f"({n} rows)")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Value formatting
    # ------------------------------------------------------------------

    def _format_value(self, val: Any) -> str:
        if val is None:
            return self.null_display
        if isinstance(val, float):
            # Show 4 decimal places for floats (matching UQA convention)
            return f"{val:.4f}"
        if isinstance(val, bool):
            return "t" if val else "f"
        return str(val)
