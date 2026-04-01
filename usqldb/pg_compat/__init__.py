#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL 17-compatible system catalog providers."""

from usqldb.pg_compat.information_schema import InformationSchemaProvider
from usqldb.pg_compat.oid import OIDAllocator
from usqldb.pg_compat.pg_catalog import PGCatalogProvider

__all__ = [
    "InformationSchemaProvider",
    "OIDAllocator",
    "PGCatalogProvider",
]
