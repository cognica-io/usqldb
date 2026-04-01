#
# usql -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL 17-compatible system catalog providers."""

from usql.pg_compat.information_schema import InformationSchemaProvider
from usql.pg_compat.oid import OIDAllocator
from usql.pg_compat.pg_catalog import PGCatalogProvider

__all__ = [
    "InformationSchemaProvider",
    "OIDAllocator",
    "PGCatalogProvider",
]
