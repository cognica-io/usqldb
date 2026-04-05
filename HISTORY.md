# History

## 0.2.0 (2026-04-05)

PostgreSQL wire protocol server, live connection monitoring, and query cancellation. All 260 tests pass.

### Wire Protocol Server

- **`usqldb.net.pgwire`** --- full PostgreSQL v3 wire protocol server supporting simple and extended query protocols.
- **4 authentication methods**: trust, cleartext password, MD5, and SCRAM-SHA-256.
- **SSL/TLS support**: optional TLS encryption via `--ssl-cert` / `--ssl-key`.
- **CLI entry point**: `usqldb-server [--host HOST] [--port PORT] [--db PATH] [--auth METHOD] [--user NAME:PASSWORD]`.
- **Programmatic API**: `PGWireServer(PGWireConfig(...))` with `asyncio.run(server.serve_forever())`.

### Live pg_stat_activity

- **`pg_stat_activity`** now shows real pgwire connections: pid, username, database, client address, query state, and current query text, populated from the connection registry.

### Query Cancellation

- **CancelRequest support**: pgwire `CancelRequest` messages cancel in-flight queries through UQA's `CancellationToken`, checked in operator hot loops.

### Connection Registry

- **`usqldb.pg_compat.connection_registry`**: Thread-safe registry tracking active pgwire sessions, used by `pg_stat_activity` and cancel-request routing.

### Catalog Improvements

- **Unqualified catalog name resolution**: `pg_catalog` and `information_schema` table names resolve without schema qualification (e.g., `SELECT * FROM pg_class` works alongside `SELECT * FROM pg_catalog.pg_class`).

### Bug Fixes

- Fixed asyncio deprecation warnings in tests.

### Tests

- **Total**: 260 tests.

## 0.1.0 (2026-04-01)

Initial release.

### Core

- `USQLEngine` — drop-in replacement for `uqa.Engine` with full PostgreSQL 17 catalog support.
- `USQLCompiler` — extended SQL compiler that intercepts `information_schema` and `pg_catalog` queries.
- `OIDAllocator` — deterministic OID assignment matching PostgreSQL 17 conventions (system 0-16383, user 16384+).

### PostgreSQL Compatibility

- 23 `information_schema` views: schemata, tables, columns, table_constraints, key_column_usage, referential_constraints, constraint_column_usage, check_constraints, views, sequences, routines, parameters, foreign_tables, foreign_servers, foreign_server_options, foreign_table_options, enabled_roles, applicable_roles, character_sets, collations, domains, element_types, triggers.
- 34 `pg_catalog` tables: pg_namespace, pg_class, pg_attribute, pg_type, pg_constraint, pg_index, pg_attrdef, pg_am, pg_database, pg_roles, pg_user, pg_tables, pg_views, pg_indexes, pg_matviews, pg_sequences, pg_settings, pg_foreign_server, pg_foreign_table, pg_foreign_data_wrapper, pg_description, pg_depend, pg_stat_user_tables, pg_stat_user_indexes, pg_stat_activity, pg_proc, pg_extension, pg_collation, pg_enum, pg_inherits, pg_trigger, pg_statio_user_tables, pg_auth_members, pg_available_extensions.
- Consistent OID cross-references across all catalog tables (e.g., pg_class.oid = pg_attribute.attrelid).

### Interactive Shell

- psql-compatible REPL with prompt_toolkit (syntax highlighting, auto-suggest, multi-line editing).
- Context-aware tab-completion for SQL keywords, table/view/column names, and backslash commands.
- Backslash commands: `\d`, `\dt`, `\di`, `\dv`, `\ds`, `\df`, `\dn`, `\du`, `\l`, `\det`, `\des`, `\dew`, `\dG`, `\x`, `\timing`, `\o`, `\i`, `\e`, `\conninfo`, `\encoding`, `\!`, `\?`, `\q`.
- psql-compatible output formatter with aligned and expanded (`\x`) display modes.
- Query timing (`\timing`), output redirection (`\o`), file execution (`\i`), and editor integration (`\e`).
- CLI entry point: `usqldb [--db PATH] [-c COMMAND] [script.sql ...]`.
