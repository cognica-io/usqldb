#
# usql -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

r"""Backslash command handlers for the usql interactive shell.

Each command queries pg_catalog / information_schema through the
USQLEngine, proving the catalog layer works and producing
psql-compatible output.

Supported commands:
    \d [NAME]       Describe relation or list all relations
    \dt[+] [PAT]    List tables
    \di[+] [PAT]    List indexes
    \dv[+] [PAT]    List views
    \ds[+] [PAT]    List sequences
    \df[+] [PAT]    List functions
    \dn[+]          List schemas
    \du             List roles
    \l              List databases
    \det            List foreign tables
    \des            List foreign servers
    \dew            List foreign data wrappers
    \dG             List named graphs
    \x              Toggle expanded display
    \timing         Toggle timing display
    \o [FILE]       Send output to file
    \i FILE         Execute commands from file
    \e [FILE]       Edit query buffer / file in $EDITOR
    \conninfo       Display connection info
    \encoding       Show client encoding
    \! CMD          Execute shell command
    \?              Show help
    \q              Quit
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from usql.cli.formatter import Formatter
    from usql.core.engine import USQLEngine


class CommandHandler:
    """Dispatches and executes backslash commands."""

    def __init__(
        self,
        engine: USQLEngine,
        formatter: Formatter,
        output_fn: Any,
    ) -> None:
        self.engine = engine
        self.formatter = formatter
        self._output = output_fn

        # Mutable state
        self.show_timing: bool = False
        self.output_file: str | None = None
        self.db_path: str | None = None

        # Execute-file callback (set by REPL)
        self.execute_file_fn: Any = None

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def handle(self, cmd_line: str) -> bool:
        """Handle a backslash command.  Returns True to quit."""
        parts = cmd_line.split(None, 1)
        verb = parts[0]
        arg = parts[1].strip() if len(parts) > 1 else ""

        if verb in ("\\q", "\\quit"):
            return True

        dispatch: dict[str, Any] = {
            "\\d": self._cmd_describe,
            "\\dt": self._cmd_list_tables,
            "\\dt+": self._cmd_list_tables_plus,
            "\\di": self._cmd_list_indexes,
            "\\di+": self._cmd_list_indexes,
            "\\dv": self._cmd_list_views,
            "\\dv+": self._cmd_list_views,
            "\\ds": self._cmd_list_sequences,
            "\\ds+": self._cmd_list_sequences,
            "\\df": self._cmd_list_functions,
            "\\df+": self._cmd_list_functions,
            "\\dn": self._cmd_list_schemas,
            "\\dn+": self._cmd_list_schemas,
            "\\du": self._cmd_list_roles,
            "\\dg": self._cmd_list_roles,
            "\\l": self._cmd_list_databases,
            "\\l+": self._cmd_list_databases,
            "\\det": self._cmd_list_foreign_tables,
            "\\des": self._cmd_list_foreign_servers,
            "\\dew": self._cmd_list_foreign_data_wrappers,
            "\\dG": self._cmd_list_graphs,
            "\\x": self._cmd_toggle_expanded,
            "\\timing": self._cmd_toggle_timing,
            "\\o": self._cmd_output,
            "\\i": self._cmd_include,
            "\\e": self._cmd_edit,
            "\\conninfo": self._cmd_conninfo,
            "\\encoding": self._cmd_encoding,
            "\\!": self._cmd_shell,
            "\\?": self._cmd_help,
            "\\h": self._cmd_help,
            "\\help": self._cmd_help,
        }

        handler = dispatch.get(verb)
        if handler is not None:
            handler(arg)
            return False

        # Try prefix match: \d<name> should resolve to \d <name>
        if verb.startswith("\\d") and len(verb) > 2 and verb not in dispatch:
            name = verb[2:] + (" " + arg if arg else "")
            self._cmd_describe(name.strip())
            return False

        self._output(f"Invalid command \\{verb[1:]}. Try \\? for help.")
        return False

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    def output(self, text: str) -> None:
        """Write text to the current output destination."""
        if self.output_file is not None:
            with open(self.output_file, "a") as f:
                f.write(text + "\n")
        else:
            self._output(text)

    def _query(self, sql: str) -> Any:
        """Execute a catalog query and return the SQLResult."""
        return self.engine.sql(sql)

    def _print_rows(
        self,
        columns: list[str],
        rows: list[dict[str, Any]],
        title: str | None = None,
    ) -> None:
        """Format and output rows."""
        self.output(self.formatter.format_rows(columns, rows, title))

    # ------------------------------------------------------------------
    # \d [NAME] -- describe or list
    # ------------------------------------------------------------------

    def _cmd_describe(self, arg: str) -> None:
        if not arg:
            self._cmd_list_relations("")
            return
        self._describe_relation(arg)

    def _cmd_list_relations(self, pattern: str) -> None:
        """List all user relations (tables, views, sequences, foreign)."""
        r = self._query(
            'SELECT c.relname AS "Name", '
            'n.nspname AS "Schema", '
            "c.relkind "
            "FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
            "WHERE n.nspname = 'public' "
            "ORDER BY c.relname"
        )
        if not r.rows:
            self.output("No relations found.")
            return

        _KINDS = {
            "r": "table",
            "v": "view",
            "i": "index",
            "S": "sequence",
            "f": "foreign table",
            "m": "materialized view",
        }
        columns = ["Schema", "Name", "Type", "Owner"]
        rows = []
        for row in r.rows:
            kind_code = row.get("relkind", "r")
            kind_label = _KINDS.get(kind_code, kind_code)
            # Filter out indexes for \d (psql behavior)
            if kind_code == "i":
                continue
            if pattern and not _like_match(row["Name"], pattern):
                continue
            rows.append(
                {
                    "Schema": row["Schema"],
                    "Name": row["Name"],
                    "Type": kind_label,
                    "Owner": "uqa",
                }
            )
        if not rows:
            self.output("No matching relations found.")
            return
        self._print_rows(columns, rows, "List of relations")

    def _describe_relation(self, name: str) -> None:
        """Describe a specific relation by name."""
        # Find the relation type
        r = self._query(
            f"SELECT c.relkind, n.nspname "
            f"FROM pg_catalog.pg_class c "
            f"JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
            f"WHERE c.relname = '{_escape(name)}'"
        )
        if not r.rows:
            self.output(f'Did not find any relation named "{name}".')
            return

        relkind = r.rows[0]["relkind"]
        schema = r.rows[0]["nspname"]

        if relkind == "r":
            self._describe_table(name, schema)
        elif relkind == "v":
            self._describe_view(name, schema)
        elif relkind == "i":
            self._describe_index(name, schema)
        elif relkind == "S":
            self._describe_sequence(name, schema)
        elif relkind == "f":
            self._describe_foreign_table(name, schema)
        else:
            self._describe_table(name, schema)

    # ------------------------------------------------------------------
    # \d TABLE -- full table description
    # ------------------------------------------------------------------

    def _describe_table(self, name: str, schema: str) -> None:
        lines: list[str] = []

        # -- Title --------------------------------------------------------
        title = f'Table "{schema}.{name}"'

        # -- Columns ------------------------------------------------------
        r = self._query(
            f"SELECT column_name, data_type, is_nullable, column_default "
            f"FROM information_schema.columns "
            f"WHERE table_name = '{_escape(name)}' "
            f"ORDER BY ordinal_position"
        )
        col_columns = ["Column", "Type", "Collation", "Nullable", "Default"]
        col_rows = []
        for row in r.rows:
            nullable = "" if row["is_nullable"] == "YES" else "not null"
            default = row.get("column_default") or ""
            col_rows.append(
                {
                    "Column": row["column_name"],
                    "Type": row["data_type"],
                    "Collation": "",
                    "Nullable": nullable,
                    "Default": str(default),
                }
            )
        lines.append(self.formatter.format_rows(col_columns, col_rows, title))
        # Remove the "(N rows)" footer from column listing
        if lines and lines[-1].endswith(" rows)"):
            parts = lines[-1].rsplit("\n", 1)
            lines[-1] = parts[0]
        elif lines and lines[-1].endswith(" row)"):
            parts = lines[-1].rsplit("\n", 1)
            lines[-1] = parts[0]

        # -- Indexes ------------------------------------------------------
        r = self._query(
            f"SELECT indexname, indexdef "
            f"FROM pg_catalog.pg_indexes "
            f"WHERE tablename = '{_escape(name)}'"
        )
        if r.rows:
            idx_lines = []
            # Classify indexes using pg_index
            r_idx = self._query(
                "SELECT c.relname, i.indisprimary, i.indisunique "
                "FROM pg_catalog.pg_index i "
                "JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid "
                "JOIN pg_catalog.pg_class t ON i.indrelid = t.oid "
                f"WHERE t.relname = '{_escape(name)}'"
            )
            idx_props: dict[str, dict[str, Any]] = {}
            for row in r_idx.rows:
                idx_props[row["relname"]] = {
                    "primary": row.get("indisprimary"),
                    "unique": row.get("indisunique"),
                }

            for row in r.rows:
                idx_name = row["indexname"]
                props = idx_props.get(idx_name, {})
                # Extract column list from indexdef
                idx_def = row["indexdef"]
                col_part = ""
                if "(" in idx_def and ")" in idx_def:
                    col_part = idx_def[idx_def.index("(") : idx_def.rindex(")") + 1]

                label_parts = []
                if props.get("primary"):
                    label_parts.append("PRIMARY KEY,")
                elif props.get("unique"):
                    label_parts.append("UNIQUE CONSTRAINT,")
                label_parts.append("btree")

                desc = " ".join(label_parts)
                idx_lines.append(f'    "{idx_name}" {desc} {col_part}')

            lines.append("Indexes:")
            lines.extend(idx_lines)

        # -- Check constraints --------------------------------------------
        r = self._query(
            f"SELECT constraint_name "
            f"FROM information_schema.table_constraints "
            f"WHERE table_name = '{_escape(name)}' "
            f"AND constraint_type = 'CHECK'"
        )
        if r.rows:
            lines.append("Check constraints:")
            for row in r.rows:
                lines.append(f'    "{row["constraint_name"]}"')

        # -- Foreign-key constraints --------------------------------------
        fk_lines = self._build_fk_lines(name)
        if fk_lines:
            lines.append("Foreign-key constraints:")
            lines.extend(fk_lines)

        # -- Referenced by ------------------------------------------------
        ref_lines = self._build_referenced_by(name)
        if ref_lines:
            lines.append("Referenced by:")
            lines.extend(ref_lines)

        self.output("\n".join(lines))

    def _build_fk_lines(self, table_name: str) -> list[str]:
        """Build FK constraint description lines for a table."""
        r = self._query(
            f"SELECT tc.constraint_name, kcu.column_name "
            f"FROM information_schema.table_constraints tc "
            f"JOIN information_schema.key_column_usage kcu "
            f"  ON tc.constraint_name = kcu.constraint_name "
            f"WHERE tc.table_name = '{_escape(table_name)}' "
            f"  AND tc.constraint_type = 'FOREIGN KEY'"
        )
        if not r.rows:
            return []

        fk_map: dict[str, str] = {}
        for row in r.rows:
            fk_map[row["constraint_name"]] = row["column_name"]

        # Get referenced table/column
        r_ref = self._query(
            "SELECT rc.constraint_name, "
            "  rc.unique_constraint_name, "
            "  ccu.table_name AS ref_table, "
            "  ccu.column_name AS ref_column "
            "FROM information_schema.referential_constraints rc "
            "JOIN information_schema.constraint_column_usage ccu "
            "  ON rc.constraint_name = ccu.constraint_name "
            "WHERE rc.constraint_name IN ("
            + ",".join(f"'{_escape(n)}'" for n in fk_map)
            + ")"
        )
        ref_map: dict[str, tuple[str, str]] = {}
        for row in r_ref.rows:
            ref_map[row["constraint_name"]] = (
                row["ref_table"],
                row["ref_column"],
            )

        result = []
        for con_name, col_name in sorted(fk_map.items()):
            ref_table, ref_col = ref_map.get(con_name, ("?", "?"))
            result.append(
                f'    "{con_name}" FOREIGN KEY ({col_name}) '
                f"REFERENCES {ref_table}({ref_col})"
            )
        return result

    def _build_referenced_by(self, table_name: str) -> list[str]:
        """Build 'Referenced by' lines (reverse FK references)."""
        r = self._query(
            f"SELECT ccu.constraint_name, "
            f"  ccu.column_name AS ref_column, "
            f"  tc.table_name AS src_table, "
            f"  kcu.column_name AS src_column "
            f"FROM information_schema.constraint_column_usage ccu "
            f"JOIN information_schema.table_constraints tc "
            f"  ON ccu.constraint_name = tc.constraint_name "
            f"JOIN information_schema.key_column_usage kcu "
            f"  ON tc.constraint_name = kcu.constraint_name "
            f"WHERE ccu.table_name = '{_escape(table_name)}' "
            f"  AND tc.constraint_type = 'FOREIGN KEY' "
            f"  AND tc.table_name != '{_escape(table_name)}'"
        )
        if not r.rows:
            return []

        result = []
        for row in r.rows:
            ref_col = row.get("ref_column", "?")
            result.append(
                f'    TABLE "{row["src_table"]}" '
                f'CONSTRAINT "{row["constraint_name"]}" '
                f"FOREIGN KEY ({row['src_column']}) "
                f"REFERENCES {table_name}({ref_col})"
            )
        return result

    # ------------------------------------------------------------------
    # \d VIEW
    # ------------------------------------------------------------------

    def _describe_view(self, name: str, schema: str) -> None:
        lines: list[str] = []
        title = f'View "{schema}.{name}"'

        # Try information_schema.columns first
        r = self._query(
            f"SELECT column_name, data_type, is_nullable "
            f"FROM information_schema.columns "
            f"WHERE table_name = '{_escape(name)}' "
            f"ORDER BY ordinal_position"
        )
        col_columns = ["Column", "Type", "Collation", "Nullable", "Default"]
        col_rows = []
        for row in r.rows:
            nullable = "" if row["is_nullable"] == "YES" else "not null"
            col_rows.append(
                {
                    "Column": row["column_name"],
                    "Type": row["data_type"],
                    "Collation": "",
                    "Nullable": nullable,
                    "Default": "",
                }
            )

        # If no columns from information_schema, try to resolve from
        # the expanded view result
        if not col_rows:
            view_table = self.engine._tables.get(name)
            if view_table is not None:
                for _pos, (cname, cdef) in enumerate(view_table.columns.items(), 1):
                    col_rows.append(
                        {
                            "Column": cname,
                            "Type": cdef.type_name,
                            "Collation": "",
                            "Nullable": "",
                            "Default": "",
                        }
                    )

        if col_rows:
            text = self.formatter.format_rows(col_columns, col_rows, title)
            text = _strip_footer(text)
            lines.append(text)
        else:
            lines.append(title)

        self.output("\n".join(lines))

    # ------------------------------------------------------------------
    # \d INDEX
    # ------------------------------------------------------------------

    def _describe_index(self, name: str, schema: str) -> None:
        r = self._query(
            f"SELECT tablename, indexdef "
            f"FROM pg_catalog.pg_indexes "
            f"WHERE indexname = '{_escape(name)}'"
        )
        if r.rows:
            row = r.rows[0]
            self.output(f'Index "{schema}.{name}"')
            self.output(f"  Table: {row['tablename']}")
            self.output(f"  Definition: {row['indexdef']}")
        else:
            self.output(f'Index "{schema}.{name}"')

    # ------------------------------------------------------------------
    # \d SEQUENCE
    # ------------------------------------------------------------------

    def _describe_sequence(self, name: str, schema: str) -> None:
        r = self._query(
            f"SELECT * FROM pg_catalog.pg_sequences "
            f"WHERE sequencename = '{_escape(name)}'"
        )
        if r.rows:
            seq = r.rows[0]
            self.output(f'Sequence "{schema}.{name}"')
            self.output(f"  Type: {seq.get('data_type', 'bigint')}")
            self.output(f"  Start: {seq.get('start_value', 1)}")
            self.output(f"  Min: {seq.get('min_value', 1)}")
            self.output(f"  Max: {seq.get('max_value', '')}")
            self.output(f"  Increment: {seq.get('increment_by', 1)}")
            self.output(f"  Cycle: {'yes' if seq.get('cycle') else 'no'}")

    # ------------------------------------------------------------------
    # \d FOREIGN TABLE
    # ------------------------------------------------------------------

    def _describe_foreign_table(self, name: str, schema: str) -> None:
        lines: list[str] = []
        title = f'Foreign table "{schema}.{name}"'

        r = self._query(
            f"SELECT column_name, data_type "
            f"FROM information_schema.columns "
            f"WHERE table_name = '{_escape(name)}' "
            f"ORDER BY ordinal_position"
        )
        col_columns = ["Column", "Type", "Collation", "Nullable", "Default"]
        col_rows = []
        for row in r.rows:
            col_rows.append(
                {
                    "Column": row["column_name"],
                    "Type": row["data_type"],
                    "Collation": "",
                    "Nullable": "",
                    "Default": "",
                }
            )
        if col_rows:
            text = self.formatter.format_rows(col_columns, col_rows, title)
            text = _strip_footer(text)
            lines.append(text)
        else:
            lines.append(title)

        # Server info
        r = self._query(
            f"SELECT foreign_server_name "
            f"FROM information_schema.foreign_tables "
            f"WHERE foreign_table_name = '{_escape(name)}'"
        )
        if r.rows:
            lines.append(f"Server: {r.rows[0]['foreign_server_name']}")

        self.output("\n".join(lines))

    # ------------------------------------------------------------------
    # \dt -- list tables
    # ------------------------------------------------------------------

    def _cmd_list_tables(self, arg: str) -> None:
        r = self._query(
            'SELECT tablename AS "Name", '
            'schemaname AS "Schema", '
            'tableowner AS "Owner" '
            "FROM pg_catalog.pg_tables "
            "WHERE schemaname = 'public' "
            "ORDER BY tablename"
        )
        rows = _filter_rows(r.rows, "Name", arg)
        cols = ["Schema", "Name", "Type", "Owner"]
        out = [
            {
                "Schema": row["Schema"],
                "Name": row["Name"],
                "Type": "table",
                "Owner": row["Owner"],
            }
            for row in rows
        ]
        if not out:
            self.output("No matching tables found.")
            return
        self._print_rows(cols, out, "List of relations")

    def _cmd_list_tables_plus(self, arg: str) -> None:
        r = self._query(
            'SELECT t.tablename AS "Name", '
            't.schemaname AS "Schema", '
            't.tableowner AS "Owner", '
            's.n_live_tup AS "Rows" '
            "FROM pg_catalog.pg_tables t "
            "LEFT JOIN pg_catalog.pg_stat_user_tables s "
            "  ON t.tablename = s.relname "
            "WHERE t.schemaname = 'public' "
            "ORDER BY t.tablename"
        )
        if not r.rows:
            # Fallback without JOIN
            self._cmd_list_tables(arg)
            return
        rows = _filter_rows(r.rows, "Name", arg)
        cols = ["Schema", "Name", "Type", "Owner", "Rows"]
        out = [
            {
                "Schema": row["Schema"],
                "Name": row["Name"],
                "Type": "table",
                "Owner": row["Owner"],
                "Rows": row.get("Rows", ""),
            }
            for row in rows
        ]
        if not out:
            self.output("No matching tables found.")
            return
        self._print_rows(cols, out, "List of relations")

    # ------------------------------------------------------------------
    # \di -- list indexes
    # ------------------------------------------------------------------

    def _cmd_list_indexes(self, arg: str) -> None:
        r = self._query(
            'SELECT indexname AS "Name", '
            'schemaname AS "Schema", '
            'tablename AS "Table" '
            "FROM pg_catalog.pg_indexes "
            "WHERE schemaname = 'public' "
            "ORDER BY indexname"
        )
        rows = _filter_rows(r.rows, "Name", arg)
        cols = ["Schema", "Name", "Type", "Owner", "Table"]
        out = [
            {
                "Schema": row["Schema"],
                "Name": row["Name"],
                "Type": "index",
                "Owner": "uqa",
                "Table": row["Table"],
            }
            for row in rows
        ]
        if not out:
            self.output("No matching indexes found.")
            return
        self._print_rows(cols, out, "List of relations")

    # ------------------------------------------------------------------
    # \dv -- list views
    # ------------------------------------------------------------------

    def _cmd_list_views(self, arg: str) -> None:
        r = self._query(
            'SELECT viewname AS "Name", '
            'schemaname AS "Schema", '
            'viewowner AS "Owner" '
            "FROM pg_catalog.pg_views "
            "WHERE schemaname = 'public' "
            "ORDER BY viewname"
        )
        rows = _filter_rows(r.rows, "Name", arg)
        cols = ["Schema", "Name", "Type", "Owner"]
        out = [
            {
                "Schema": row["Schema"],
                "Name": row["Name"],
                "Type": "view",
                "Owner": row["Owner"],
            }
            for row in rows
        ]
        if not out:
            self.output("No matching views found.")
            return
        self._print_rows(cols, out, "List of relations")

    # ------------------------------------------------------------------
    # \ds -- list sequences
    # ------------------------------------------------------------------

    def _cmd_list_sequences(self, arg: str) -> None:
        r = self._query(
            'SELECT sequencename AS "Name", '
            'schemaname AS "Schema", '
            'sequenceowner AS "Owner" '
            "FROM pg_catalog.pg_sequences "
            "WHERE schemaname = 'public' "
            "ORDER BY sequencename"
        )
        rows = _filter_rows(r.rows, "Name", arg)
        cols = ["Schema", "Name", "Type", "Owner"]
        out = [
            {
                "Schema": row["Schema"],
                "Name": row["Name"],
                "Type": "sequence",
                "Owner": row["Owner"],
            }
            for row in rows
        ]
        if not out:
            self.output("No matching sequences found.")
            return
        self._print_rows(cols, out, "List of relations")

    # ------------------------------------------------------------------
    # \df -- list functions
    # ------------------------------------------------------------------

    def _cmd_list_functions(self, arg: str) -> None:
        r = self._query(
            'SELECT proname AS "Name", '
            'pronargs AS "Args" '
            "FROM pg_catalog.pg_proc "
            "ORDER BY proname"
        )
        rows = _filter_rows(r.rows, "Name", arg)
        cols = ["Schema", "Name", "Result data type", "Argument data types"]
        out = [
            {
                "Schema": "public",
                "Name": row["Name"],
                "Result data type": "text",
                "Argument data types": f"({row['Args']} args)",
            }
            for row in rows
        ]
        if not out:
            self.output("No matching functions found.")
            return
        self._print_rows(cols, out, "List of functions")

    # ------------------------------------------------------------------
    # \dn -- list schemas
    # ------------------------------------------------------------------

    def _cmd_list_schemas(self, arg: str) -> None:
        r = self._query(
            'SELECT nspname AS "Name" FROM pg_catalog.pg_namespace ORDER BY nspname'
        )
        cols = ["Name", "Owner"]
        out = [{"Name": row["Name"], "Owner": "uqa"} for row in r.rows]
        self._print_rows(cols, out, "List of schemas")

    # ------------------------------------------------------------------
    # \du -- list roles
    # ------------------------------------------------------------------

    def _cmd_list_roles(self, arg: str) -> None:
        r = self._query(
            'SELECT rolname AS "Name", '
            "rolsuper, rolcreaterole, rolcreatedb, "
            "rolcanlogin, rolreplication, rolconnlimit "
            "FROM pg_catalog.pg_roles "
            "ORDER BY rolname"
        )
        cols = [
            "Role name",
            "Superuser",
            "Create role",
            "Create DB",
            "Login",
            "Replication",
            "Conn limit",
        ]
        out = []
        for row in r.rows:
            out.append(
                {
                    "Role name": row["Name"],
                    "Superuser": _yn(row.get("rolsuper")),
                    "Create role": _yn(row.get("rolcreaterole")),
                    "Create DB": _yn(row.get("rolcreatedb")),
                    "Login": _yn(row.get("rolcanlogin")),
                    "Replication": _yn(row.get("rolreplication")),
                    "Conn limit": row.get("rolconnlimit", -1),
                }
            )
        self._print_rows(cols, out, "List of roles")

    # ------------------------------------------------------------------
    # \l -- list databases
    # ------------------------------------------------------------------

    def _cmd_list_databases(self, arg: str) -> None:
        r = self._query(
            'SELECT datname AS "Name", '
            'encoding AS "Encoding", '
            'datcollate AS "Collate", '
            'datctype AS "Ctype" '
            "FROM pg_catalog.pg_database"
        )
        cols = ["Name", "Owner", "Encoding", "Collate", "Ctype"]
        out = [
            {
                "Name": row["Name"],
                "Owner": "uqa",
                "Encoding": "UTF8",
                "Collate": row.get("Collate", ""),
                "Ctype": row.get("Ctype", ""),
            }
            for row in r.rows
        ]
        self._print_rows(cols, out, "List of databases")

    # ------------------------------------------------------------------
    # \det -- list foreign tables
    # ------------------------------------------------------------------

    def _cmd_list_foreign_tables(self, arg: str) -> None:
        r = self._query(
            'SELECT foreign_table_name AS "Name", '
            'foreign_table_schema AS "Schema", '
            'foreign_server_name AS "Server" '
            "FROM information_schema.foreign_tables "
            "ORDER BY foreign_table_name"
        )
        if not r.rows:
            self.output("No foreign tables found.")
            return
        cols = ["Schema", "Name", "Server"]
        self._print_rows(cols, r.rows, "List of foreign tables")

    # ------------------------------------------------------------------
    # \des -- list foreign servers
    # ------------------------------------------------------------------

    def _cmd_list_foreign_servers(self, arg: str) -> None:
        r = self._query(
            'SELECT foreign_server_name AS "Name", '
            'foreign_data_wrapper_name AS "FDW" '
            "FROM information_schema.foreign_servers "
            "ORDER BY foreign_server_name"
        )
        if not r.rows:
            self.output("No foreign servers found.")
            return
        cols = ["Name", "Owner", "FDW"]
        out = [
            {"Name": row["Name"], "Owner": "uqa", "FDW": row.get("FDW", "")}
            for row in r.rows
        ]
        self._print_rows(cols, out, "List of foreign servers")

    # ------------------------------------------------------------------
    # \dew -- list foreign data wrappers
    # ------------------------------------------------------------------

    def _cmd_list_foreign_data_wrappers(self, arg: str) -> None:
        r = self._query(
            'SELECT fdwname AS "Name" '
            "FROM pg_catalog.pg_foreign_data_wrapper "
            "ORDER BY fdwname"
        )
        if not r.rows:
            self.output("No foreign data wrappers found.")
            return
        cols = ["Name", "Owner"]
        out = [{"Name": row["Name"], "Owner": "uqa"} for row in r.rows]
        self._print_rows(cols, out, "List of foreign-data wrappers")

    # ------------------------------------------------------------------
    # \dG -- list named graphs (UQA extension)
    # ------------------------------------------------------------------

    def _cmd_list_graphs(self, arg: str) -> None:
        gs = self.engine._graph_store
        names = gs.graph_names()
        if not names:
            self.output("No named graphs.")
            return
        cols = ["Graph", "Vertices", "Edges"]
        out = []
        for name in sorted(names):
            out.append(
                {
                    "Graph": name,
                    "Vertices": len(gs.vertex_ids_in_graph(name)),
                    "Edges": len(gs.edges_in_graph(name)),
                }
            )
        self._print_rows(cols, out, "List of named graphs")

    # ------------------------------------------------------------------
    # \x -- toggle expanded display
    # ------------------------------------------------------------------

    def _cmd_toggle_expanded(self, arg: str) -> None:
        self.formatter.expanded = not self.formatter.expanded
        state = "on" if self.formatter.expanded else "off"
        self.output(f"Expanded display is {state}.")

    # ------------------------------------------------------------------
    # \timing -- toggle timing
    # ------------------------------------------------------------------

    def _cmd_toggle_timing(self, arg: str) -> None:
        self.show_timing = not self.show_timing
        state = "on" if self.show_timing else "off"
        self.output(f"Timing is {state}.")

    # ------------------------------------------------------------------
    # \o [FILE] -- output to file
    # ------------------------------------------------------------------

    def _cmd_output(self, arg: str) -> None:
        if arg:
            self.output_file = arg
            self.output(f"Output redirected to: {arg}")
        else:
            if self.output_file is not None:
                self.output(f"Output restored to stdout (was: {self.output_file}).")
            self.output_file = None

    # ------------------------------------------------------------------
    # \i FILE -- include/execute file
    # ------------------------------------------------------------------

    def _cmd_include(self, arg: str) -> None:
        if not arg:
            self.output("Usage: \\i <filename>")
            return
        if not os.path.isfile(arg):
            self.output(f"File not found: {arg}")
            return
        if self.execute_file_fn is not None:
            self.execute_file_fn(arg)

    # ------------------------------------------------------------------
    # \e [FILE] -- edit in $EDITOR
    # ------------------------------------------------------------------

    def _cmd_edit(self, arg: str) -> None:
        editor = os.environ.get("EDITOR", os.environ.get("VISUAL", "vi"))
        if arg:
            path = arg
        else:
            fd, path = tempfile.mkstemp(suffix=".sql")
            os.close(fd)

        try:
            subprocess.call([editor, path])
        except FileNotFoundError:
            self.output(f"Editor not found: {editor}")
            return

        if not arg and os.path.isfile(path):
            if self.execute_file_fn is not None:
                self.execute_file_fn(path)
            try:
                os.unlink(path)
            except OSError:
                pass

    # ------------------------------------------------------------------
    # \conninfo -- connection info
    # ------------------------------------------------------------------

    def _cmd_conninfo(self, arg: str) -> None:
        db = self.db_path or ":memory:"
        self.output(f'You are connected to database "uqa" via file "{db}".')

    # ------------------------------------------------------------------
    # \encoding -- client encoding
    # ------------------------------------------------------------------

    def _cmd_encoding(self, arg: str) -> None:
        self.output("UTF8")

    # ------------------------------------------------------------------
    # \! -- shell command
    # ------------------------------------------------------------------

    def _cmd_shell(self, arg: str) -> None:
        if arg:
            os.system(arg)
        else:
            shell = os.environ.get("SHELL", "/bin/sh")
            os.system(shell)

    # ------------------------------------------------------------------
    # \? -- help
    # ------------------------------------------------------------------

    def _cmd_help(self, arg: str) -> None:
        self.output(
            "General\n"
            "  \\q                  Quit\n"
            "  \\? [commands]       Show help\n"
            "  \\conninfo           Display connection info\n"
            "  \\encoding           Show client encoding\n"
            "  \\! [COMMAND]        Execute shell command\n"
            "\n"
            "Informational\n"
            "  \\d [NAME]           Describe table/view/index or list all\n"
            "  \\dt[+] [PATTERN]    List tables\n"
            "  \\di[+] [PATTERN]    List indexes\n"
            "  \\dv[+] [PATTERN]    List views\n"
            "  \\ds[+] [PATTERN]    List sequences\n"
            "  \\df[+] [PATTERN]    List functions\n"
            "  \\dn[+]              List schemas\n"
            "  \\du                 List roles\n"
            "  \\l[+]               List databases\n"
            "  \\det                List foreign tables\n"
            "  \\des                List foreign servers\n"
            "  \\dew                List foreign data wrappers\n"
            "  \\dG                 List named graphs\n"
            "\n"
            "Formatting\n"
            "  \\x                  Toggle expanded display\n"
            "  \\timing             Toggle timing of commands\n"
            "\n"
            "Input/Output\n"
            "  \\o [FILE]           Send output to file or stdout\n"
            "  \\i FILE             Execute commands from file\n"
            "  \\e [FILE]           Edit query or file with $EDITOR"
        )


# ======================================================================
# Helpers
# ======================================================================


def _escape(s: str) -> str:
    """Escape single quotes for use in SQL string literals."""
    return s.replace("'", "''")


def _like_match(value: str, pattern: str) -> bool:
    """Simple pattern matching (case-insensitive substring)."""
    return pattern.lower() in value.lower()


def _filter_rows(
    rows: list[dict[str, Any]], key: str, pattern: str
) -> list[dict[str, Any]]:
    """Filter rows by pattern on a given key."""
    if not pattern:
        return rows
    return [r for r in rows if _like_match(str(r.get(key, "")), pattern)]


def _strip_footer(text: str) -> str:
    """Remove the (N rows) footer from formatted output."""
    lines = text.split("\n")
    if lines and (lines[-1].startswith("(") and lines[-1].endswith(")")):
        return "\n".join(lines[:-1])
    return text


def _yn(val: Any) -> str:
    """Convert a boolean-ish value to 'yes'/'no' display."""
    if val in (True, 1, "1", "t", "true"):
        return "yes"
    return "no"
