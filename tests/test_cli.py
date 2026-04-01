#
# usql -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Tests for the usql CLI module (formatter, commands, REPL)."""

from __future__ import annotations

import pytest

from usql.cli.commands import CommandHandler
from usql.cli.formatter import Formatter
from usql.cli.repl import USQLShell
from usql.core.engine import USQLEngine

# ======================================================================
# Fixtures
# ======================================================================


@pytest.fixture
def engine():
    e = USQLEngine()
    e.sql(
        "CREATE TABLE departments (  id SERIAL PRIMARY KEY,  name TEXT NOT NULL UNIQUE)"
    )
    e.sql(
        "CREATE TABLE employees ("
        "  id SERIAL PRIMARY KEY,"
        "  dept_id INTEGER REFERENCES departments(id),"
        "  name TEXT NOT NULL,"
        "  email TEXT UNIQUE,"
        "  salary NUMERIC(10,2)"
        ")"
    )
    e.sql("CREATE VIEW dept_summary AS SELECT name FROM departments")
    e.sql("CREATE SEQUENCE invoice_seq START 1000 INCREMENT 5")
    e.sql("INSERT INTO departments (name) VALUES ('Engineering')")
    e.sql("INSERT INTO departments (name) VALUES ('Sales')")
    e.sql(
        "INSERT INTO employees (dept_id, name, email, salary) "
        "VALUES (1, 'Alice', 'alice@ex.com', 150000.50)"
    )
    return e


@pytest.fixture
def shell(engine):
    """USQLShell with pre-built schema, capturing output."""
    s = USQLShell()
    s._engine = engine
    s._commands.engine = engine
    return s


@pytest.fixture
def captured(engine):
    """CommandHandler that captures output to a list."""
    lines: list[str] = []
    fmt = Formatter()
    handler = CommandHandler(engine, fmt, lines.append)
    return handler, lines


# ======================================================================
# Formatter tests
# ======================================================================


class TestFormatter:
    def test_aligned_basic(self):
        fmt = Formatter()
        text = fmt.format_rows(
            ["name", "age"],
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
        )
        assert "Alice" in text
        assert "Bob" in text
        assert "(2 rows)" in text

    def test_aligned_title(self):
        fmt = Formatter()
        text = fmt.format_rows(["x"], [{"x": 1}], title="Test Title")
        assert "Test Title" in text

    def test_aligned_empty(self):
        fmt = Formatter()
        text = fmt.format_rows(["x"], [])
        assert "(0 rows)" in text

    def test_aligned_single_row(self):
        fmt = Formatter()
        text = fmt.format_rows(["x"], [{"x": 1}])
        assert "(1 row)" in text

    def test_expanded_mode(self):
        fmt = Formatter()
        fmt.expanded = True
        text = fmt.format_rows(
            ["name", "age"],
            [{"name": "Alice", "age": 30}],
        )
        assert "-[ RECORD 1 ]" in text
        assert "name" in text
        assert "Alice" in text
        assert "(1 row)" in text

    def test_null_display(self):
        fmt = Formatter()
        fmt.null_display = "[NULL]"
        text = fmt.format_rows(["x"], [{"x": None}])
        assert "[NULL]" in text

    def test_float_formatting(self):
        fmt = Formatter()
        text = fmt.format_rows(["val"], [{"val": 3.14}])
        assert "3.1400" in text

    def test_format_result(self, engine):
        fmt = Formatter()
        result = engine.sql("SELECT 1 AS num")
        text = fmt.format_result(result)
        assert "num" in text
        assert "1" in text


# ======================================================================
# Command handler tests
# ======================================================================


class TestCommandListTables:
    def test_dt(self, captured):
        handler, lines = captured
        handler.handle("\\dt")
        output = "\n".join(lines)
        assert "departments" in output
        assert "employees" in output
        assert "table" in output

    def test_dt_with_pattern(self, captured):
        handler, lines = captured
        handler.handle("\\dt emp")
        output = "\n".join(lines)
        assert "employees" in output
        assert "departments" not in output

    def test_dt_plus(self, captured):
        handler, lines = captured
        handler.handle("\\dt+")
        output = "\n".join(lines)
        assert "departments" in output


class TestCommandListIndexes:
    def test_di(self, captured):
        handler, lines = captured
        handler.handle("\\di")
        output = "\n".join(lines)
        assert "departments_pkey" in output
        assert "employees_email_key" in output
        assert "index" in output


class TestCommandListViews:
    def test_dv(self, captured):
        handler, lines = captured
        handler.handle("\\dv")
        output = "\n".join(lines)
        assert "dept_summary" in output
        assert "view" in output


class TestCommandListSequences:
    def test_ds(self, captured):
        handler, lines = captured
        handler.handle("\\ds")
        output = "\n".join(lines)
        assert "invoice_seq" in output
        assert "sequence" in output


class TestCommandListSchemas:
    def test_dn(self, captured):
        handler, lines = captured
        handler.handle("\\dn")
        output = "\n".join(lines)
        assert "public" in output
        assert "pg_catalog" in output
        assert "information_schema" in output


class TestCommandListRoles:
    def test_du(self, captured):
        handler, lines = captured
        handler.handle("\\du")
        output = "\n".join(lines)
        assert "uqa" in output
        assert "yes" in output  # superuser


class TestCommandListDatabases:
    def test_l(self, captured):
        handler, lines = captured
        handler.handle("\\l")
        output = "\n".join(lines)
        assert "uqa" in output
        assert "UTF8" in output


class TestCommandDescribe:
    def test_d_no_args_lists_all(self, captured):
        handler, lines = captured
        handler.handle("\\d")
        output = "\n".join(lines)
        assert "departments" in output
        assert "employees" in output
        assert "dept_summary" in output
        assert "invoice_seq" in output

    def test_d_table(self, captured):
        handler, lines = captured
        handler.handle("\\d employees")
        output = "\n".join(lines)
        # Title
        assert 'Table "public.employees"' in output
        # Columns
        assert "id" in output
        assert "dept_id" in output
        assert "salary" in output
        assert "integer" in output
        assert "numeric" in output
        assert "not null" in output
        # Indexes
        assert "employees_pkey" in output
        assert "PRIMARY KEY" in output
        assert "employees_email_key" in output
        assert "UNIQUE CONSTRAINT" in output
        # FK
        assert "employees_dept_id_fkey" in output
        assert "FOREIGN KEY" in output
        assert "REFERENCES departments" in output

    def test_d_table_referenced_by(self, captured):
        handler, lines = captured
        handler.handle("\\d departments")
        output = "\n".join(lines)
        assert "Referenced by:" in output
        assert "employees" in output
        assert "employees_dept_id_fkey" in output

    def test_d_sequence(self, captured):
        handler, lines = captured
        handler.handle("\\d invoice_seq")
        output = "\n".join(lines)
        assert 'Sequence "public.invoice_seq"' in output
        assert "1000" in output
        assert "5" in output

    def test_d_index(self, captured):
        handler, lines = captured
        handler.handle("\\d employees_pkey")
        output = "\n".join(lines)
        assert 'Index "public.employees_pkey"' in output
        assert "employees" in output

    def test_d_view(self, captured):
        handler, lines = captured
        handler.handle("\\d dept_summary")
        output = "\n".join(lines)
        assert 'View "public.dept_summary"' in output

    def test_d_nonexistent(self, captured):
        handler, lines = captured
        handler.handle("\\d nonexistent")
        output = "\n".join(lines)
        assert "Did not find" in output


class TestCommandToggle:
    def test_x_toggle(self, captured):
        handler, lines = captured
        assert not handler.formatter.expanded
        handler.handle("\\x")
        assert handler.formatter.expanded
        assert "on" in lines[-1]
        handler.handle("\\x")
        assert not handler.formatter.expanded
        assert "off" in lines[-1]

    def test_timing_toggle(self, captured):
        handler, lines = captured
        assert not handler.show_timing
        handler.handle("\\timing")
        assert handler.show_timing
        assert "on" in lines[-1]


class TestCommandMisc:
    def test_conninfo(self, captured):
        handler, lines = captured
        handler.handle("\\conninfo")
        assert "uqa" in lines[-1]

    def test_encoding(self, captured):
        handler, lines = captured
        handler.handle("\\encoding")
        assert "UTF8" in lines[-1]

    def test_help(self, captured):
        handler, lines = captured
        handler.handle("\\?")
        output = "\n".join(lines)
        assert "\\dt" in output
        assert "\\d " in output
        assert "\\q" in output

    def test_quit(self, captured):
        handler, _lines = captured
        should_quit = handler.handle("\\q")
        assert should_quit is True

    def test_invalid_command(self, captured):
        handler, lines = captured
        handler.handle("\\zzz")
        assert "Invalid" in lines[-1] or "Try" in lines[-1]

    def test_output_redirect(self, captured, tmp_path):
        handler, _lines = captured
        outfile = str(tmp_path / "out.txt")
        handler.handle(f"\\o {outfile}")
        assert handler.output_file == outfile
        handler.handle("\\o")
        assert handler.output_file is None


class TestCommandListAll:
    def test_d_excludes_indexes(self, captured):
        """\\d without args should not list indexes (psql behavior)."""
        handler, lines = captured
        handler.handle("\\d")
        output = "\n".join(lines)
        # Should show tables, views, sequences but NOT individual indexes
        assert "departments" in output
        assert "employees_pkey" not in output  # index excluded


# ======================================================================
# Shell integration tests
# ======================================================================


class TestShellExecution:
    def test_execute_select(self, shell, capsys):
        shell._execute_one("SELECT 1 AS answer")
        captured = capsys.readouterr()
        assert "answer" in captured.out
        assert "1" in captured.out

    def test_execute_create_and_query(self, shell, capsys):
        shell._execute_one("SELECT name FROM departments ORDER BY name")
        captured = capsys.readouterr()
        assert "Engineering" in captured.out
        assert "Sales" in captured.out

    def test_execute_error(self, shell, capsys):
        shell._execute_one("SELECT * FROM nonexistent_table")
        captured = capsys.readouterr()
        assert "ERROR" in captured.out

    def test_run_file(self, shell, capsys, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 42 AS magic;")
        shell.run_file(str(sql_file))
        captured = capsys.readouterr()
        assert "42" in captured.out

    def test_timing(self, shell, capsys):
        shell._commands.show_timing = True
        shell._execute_one("SELECT 1")
        captured = capsys.readouterr()
        assert "Time:" in captured.out
        assert "ms" in captured.out
