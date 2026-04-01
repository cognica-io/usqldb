# usql

PostgreSQL 17-compatible `information_schema` and `pg_catalog` layer for [UQA](https://github.com/cognica-io/uqa).

usql extends the UQA SQL engine with a comprehensive set of PostgreSQL system catalog views so that standard PostgreSQL tools — psql, SQLAlchemy, DBeaver, DataGrip, Django, and others — can introspect the database as if it were a real PostgreSQL 17 instance.

## Features

- **23 information_schema views** — schemata, tables, columns, constraints, views, sequences, routines, foreign tables, triggers, and more.
- **34 pg_catalog tables** — pg_class, pg_attribute, pg_type, pg_constraint, pg_index, pg_proc, pg_settings, statistics views, and more, with consistent OID cross-references across all of them.
- **Interactive SQL shell** — psql-style REPL with syntax highlighting, tab-completion, backslash commands, expanded display, query timing, and multi-line editing.
- **Drop-in engine** — `USQLEngine` is a drop-in replacement for `uqa.Engine`. Import it, and every query gets full catalog support.

## Requirements

- Python 3.12+
- UQA >= 0.21.0

## Installation

```bash
pip install usql
```

## Quick Start

### As a library

```python
from usql import USQLEngine

engine = USQLEngine()
engine.sql("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)")
engine.sql("INSERT INTO users (name) VALUES ('Alice')")

# information_schema
result = engine.sql(
    "SELECT column_name, data_type "
    "FROM information_schema.columns "
    "WHERE table_name = 'users'"
)

# pg_catalog with OID joins
result = engine.sql(
    "SELECT c.relname, a.attname, t.typname "
    "FROM pg_catalog.pg_class c "
    "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid "
    "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid "
    "WHERE c.relname = 'users' AND a.attnum > 0"
)
```

### As a CLI

```bash
# In-memory database
usql

# Persistent storage
usql --db mydata.db

# Execute a SQL script then enter REPL
usql script.sql

# Execute a single command and exit
usql -c "SELECT 1"
```

### Backslash Commands

```
General
  \q                  Quit
  \? [commands]       Show help
  \conninfo           Display connection info
  \encoding           Show client encoding
  \! [COMMAND]        Execute shell command

Informational
  \d [NAME]           Describe table/view/index or list all
  \dt[+] [PATTERN]    List tables
  \di[+] [PATTERN]    List indexes
  \dv[+] [PATTERN]    List views
  \ds[+] [PATTERN]    List sequences
  \df[+] [PATTERN]    List functions
  \dn[+]              List schemas
  \du                 List roles
  \l[+]               List databases
  \det                List foreign tables
  \des                List foreign servers
  \dew                List foreign data wrappers
  \dG                 List named graphs

Formatting
  \x                  Toggle expanded display
  \timing             Toggle timing of commands

Input/Output
  \o [FILE]           Send output to file or stdout
  \i FILE             Execute commands from file
  \e [FILE]           Edit query or file with $EDITOR
```

## Project Structure

```
usql/
  __init__.py              Package root, exports USQLEngine
  core/
    engine.py              USQLEngine — drop-in replacement for uqa.Engine
    compiler.py            Extended SQL compiler with catalog support
  pg_compat/
    oid.py                 OID allocation and PostgreSQL type mapping
    information_schema.py  23 information_schema view builders
    pg_catalog.py          34 pg_catalog table builders
  cli/
    repl.py                Interactive REPL with prompt_toolkit
    commands.py            Backslash command handlers
    formatter.py           psql-compatible tabular and expanded output
    completer.py           Context-aware SQL tab-completion
```

## Development

```bash
# Install in editable mode
pip install -e .

# Run tests
pytest

# Lint and format
ruff check usql tests
ruff format usql tests

# Type check
pyright
```

## License

AGPL-3.0-only

## Author

Jaepil Jeong (jaepil@cognica.io) — [Cognica, Inc.](https://github.com/cognica-io)
