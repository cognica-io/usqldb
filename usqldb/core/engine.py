#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL 17-compatible UQA Engine.

Extends the standard UQA Engine so that every SQL query uses the
USQLCompiler instead of the base SQLCompiler.  This provides
comprehensive information_schema and pg_catalog support without
changing any other Engine behavior.
"""

from __future__ import annotations

from typing import Any

from uqa.engine import Engine

from usqldb.core.compiler import USQLCompiler


class USQLEngine(Engine):
    """UQA Engine with PostgreSQL 17-compatible system catalogs.

    Drop-in replacement for ``uqa.Engine``.  All SQL queries are
    compiled with ``USQLCompiler``, which provides full
    information_schema and pg_catalog coverage.

    Usage::

        from usqldb import USQLEngine

        engine = USQLEngine()
        engine.sql("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)")
        engine.sql("INSERT INTO users (name) VALUES ('Alice')")

        # Full information_schema support
        result = engine.sql(
            "SELECT * FROM information_schema.columns "
            "WHERE table_name = 'users'"
        )

        # Full pg_catalog support with OID cross-references
        result = engine.sql(
            "SELECT c.relname, a.attname, t.typname "
            "FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid "
            "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid "
            "WHERE c.relname = 'users' AND a.attnum > 0"
        )
    """

    def sql(self, query: str, params: list[Any] | None = None) -> Any:
        """Execute a SQL query with PostgreSQL 17-compatible catalogs."""
        compiler = USQLCompiler(self)
        return compiler.execute(query, params=params)
