#
# usql -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL 17-compatible information_schema and pg_catalog for UQA.

This package extends UQA's SQL compiler with a comprehensive set of
PostgreSQL system catalog views, enabling compatibility with standard
PostgreSQL tools (psql, SQLAlchemy, DBeaver, DataGrip, Django, etc.).
"""

__version__ = "0.1.0"

from usql.core.engine import USQLEngine

__all__ = ["USQLEngine"]
