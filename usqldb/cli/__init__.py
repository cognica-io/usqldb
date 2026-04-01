#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Interactive SQL shell with PostgreSQL 17-compatible catalog commands."""

from usqldb.cli.repl import USQLShell, main

__all__ = ["USQLShell", "main"]
