#
# usql -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Core engine and compiler extending UQA with PostgreSQL 17 compatibility."""

from usql.core.compiler import USQLCompiler
from usql.core.engine import USQLEngine

__all__ = ["USQLCompiler", "USQLEngine"]
