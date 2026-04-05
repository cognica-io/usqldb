#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Integration tests for PGWireServer with psycopg client."""

from __future__ import annotations

import asyncio
import threading

import psycopg
import pytest

from usqldb.core.engine import USQLEngine
from usqldb.net.pgwire._config import PGWireConfig
from usqldb.net.pgwire._server import PGWireServer


class ServerFixture:
    """Helper to run a PGWireServer in a background thread."""

    def __init__(self, config: PGWireConfig) -> None:
        self.config = config
        self.server: PGWireServer | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self.port = 0

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._started.wait(timeout=10):
            raise RuntimeError("Server failed to start")

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        async def _start_and_signal() -> None:
            self.server = PGWireServer(self.config)
            await self.server.start()
            self.port = self.server.port
            self._started.set()
            # Block until the task is cancelled by stop().
            await asyncio.Event().wait()

        self._main_task = self._loop.create_task(_start_and_signal())
        try:
            self._loop.run_until_complete(self._main_task)
        except asyncio.CancelledError:
            pass
        finally:
            # Drain remaining callbacks before closing.
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()

    def stop(self) -> None:
        if self._loop and self.server:

            async def _stop() -> None:
                assert self.server is not None
                await self.server.stop()

            future = asyncio.run_coroutine_threadsafe(_stop(), self._loop)
            try:
                future.result(timeout=5)
            except Exception:
                pass

        if self._loop and hasattr(self, "_main_task"):
            self._loop.call_soon_threadsafe(self._main_task.cancel)
        if self._thread:
            self._thread.join(timeout=5)

    def conninfo(self) -> str:
        return f"host=127.0.0.1 port={self.port} user=uqa dbname=uqa"


@pytest.fixture
def pgserver():
    """Start a pgwire server on an ephemeral port."""
    config = PGWireConfig(host="127.0.0.1", port=0)
    srv = ServerFixture(config)
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture
def shared_pgserver():
    """Start a pgwire server with a shared engine for all connections."""
    shared_engine = USQLEngine()
    config = PGWireConfig(
        host="127.0.0.1",
        port=0,
        engine_factory=lambda: shared_engine,
    )
    srv = ServerFixture(config)
    srv.start()
    yield srv
    srv.stop()


class TestBasicConnection:
    def test_connect_and_disconnect(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        assert conn.info.server_version >= 170000
        conn.close()

    def test_simple_select(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        cur = conn.execute("SELECT 1 AS num")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
        conn.close()

    def test_select_multiple_columns(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        cur = conn.execute("SELECT 1 AS a, 'hello' AS b, 3.14 AS c")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == "hello"
        conn.close()


class TestDDLAndDML:
    def test_create_table_and_insert(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        conn.execute(
            "CREATE TABLE test_table (  id SERIAL PRIMARY KEY,  name TEXT NOT NULL)"
        )
        conn.execute("INSERT INTO test_table (name) VALUES ('Alice')")
        conn.execute("INSERT INTO test_table (name) VALUES ('Bob')")

        cur = conn.execute("SELECT name FROM test_table ORDER BY id")
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "Alice"
        assert rows[1][0] == "Bob"
        conn.close()

    def test_update(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        conn.execute("CREATE TABLE upd (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO upd (id, val) VALUES (1, 'old')")
        conn.execute("UPDATE upd SET val = 'new' WHERE id = 1")
        cur = conn.execute("SELECT val FROM upd WHERE id = 1")
        assert cur.fetchone()[0] == "new"
        conn.close()

    def test_delete(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        conn.execute("CREATE TABLE del_test (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO del_test (id) VALUES (1)")
        conn.execute("INSERT INTO del_test (id) VALUES (2)")
        conn.execute("DELETE FROM del_test WHERE id = 1")
        cur = conn.execute("SELECT COUNT(*) FROM del_test")
        assert cur.fetchone()[0] == 1
        conn.close()


class TestPGCatalog:
    def test_pg_tables(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        conn.execute("CREATE TABLE catalog_test (id INTEGER PRIMARY KEY)")
        cur = conn.execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'"
        )
        tables = {row[0] for row in cur.fetchall()}
        assert "catalog_test" in tables
        conn.close()

    def test_information_schema_columns(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        conn.execute("CREATE TABLE col_test (id INTEGER, name TEXT, score REAL)")
        cur = conn.execute(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = 'col_test' "
            "ORDER BY ordinal_position"
        )
        rows = cur.fetchall()
        assert len(rows) == 3
        assert rows[0][0] == "id"
        assert rows[1][0] == "name"
        assert rows[2][0] == "score"
        conn.close()


class TestSetShow:
    def test_set_and_show(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        conn.execute("SET search_path TO public")
        cur = conn.execute("SHOW search_path")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "public"
        conn.close()

    def test_show_server_version(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        cur = conn.execute("SHOW server_version")
        row = cur.fetchone()
        assert row is not None
        assert "17" in row[0]
        conn.close()


class TestExtendedProtocol:
    def test_prepared_statement(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        conn.execute("CREATE TABLE prep_test (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO prep_test (id, name) VALUES (1, 'Alice')")
        conn.execute("INSERT INTO prep_test (id, name) VALUES (2, 'Bob')")

        # psycopg uses extended protocol for parameterized queries.
        cur = conn.execute("SELECT name FROM prep_test WHERE id = %s", [1])
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "Alice"
        conn.close()


class TestEmptyQuery:
    def test_empty_string(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        cur = conn.execute("")
        # psycopg returns EMPTY_QUERY status -- no rows to fetch.
        assert cur.statusmessage is None or cur.statusmessage == ""
        conn.close()

    def test_semicolons_only(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        # Semicolons-only is treated as empty by pglast.split().
        cur = conn.execute(";;;")
        assert cur.statusmessage is None or cur.statusmessage == ""
        conn.close()


class TestMultipleConnections:
    def test_two_connections(self, pgserver):
        conn1 = psycopg.connect(pgserver.conninfo(), autocommit=True)
        conn2 = psycopg.connect(pgserver.conninfo(), autocommit=True)

        conn1.execute("CREATE TABLE multi_test (id INTEGER)")
        conn1.execute("INSERT INTO multi_test (id) VALUES (1)")

        # conn2 has its own engine, so it won't see conn1's table.
        # This is expected with per-connection engines.
        conn1.close()
        conn2.close()


class TestErrorHandling:
    def test_syntax_error(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        with pytest.raises(psycopg.errors.Error):
            conn.execute("SELET 1")
        # Connection should still be usable after error.
        cur = conn.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
        conn.close()

    def test_table_not_found(self, pgserver):
        conn = psycopg.connect(pgserver.conninfo(), autocommit=True)
        with pytest.raises(psycopg.errors.Error):
            conn.execute("SELECT * FROM nonexistent_table")
        conn.close()
