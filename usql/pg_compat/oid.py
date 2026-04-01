#
# usql -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL OID allocation and type mapping.

PostgreSQL assigns a unique OID (Object Identifier) to every database
object: types, tables, schemas, indexes, constraints, functions, etc.
Tools that inspect the catalog rely on OIDs being consistent across
JOINs (e.g. pg_class.oid = pg_attribute.attrelid).

OID ranges:
    0-16383     Reserved for system objects (built-in types, schemas)
    16384+      User-defined objects (tables, indexes, constraints)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uqa.engine import Engine

# ======================================================================
# Built-in type OIDs (matching PostgreSQL 17)
# ======================================================================

TYPE_OIDS: dict[str, int] = {
    "boolean": 16,
    "bool": 16,
    "bytea": 17,
    "name": 19,
    "bigint": 20,
    "int8": 20,
    "smallint": 21,
    "int2": 21,
    "integer": 23,
    "int": 23,
    "int4": 23,
    "oid": 26,
    "text": 25,
    "json": 114,
    "xml": 142,
    "point": 600,
    "real": 700,
    "float": 700,
    "float4": 700,
    "double precision": 701,
    "float8": 701,
    "character": 1042,
    "char": 1042,
    "character varying": 1043,
    "varchar": 1043,
    "date": 1082,
    "time": 1083,
    "timestamp": 1114,
    "timestamp without time zone": 1114,
    "timestamptz": 1184,
    "timestamp with time zone": 1184,
    "interval": 1186,
    "numeric": 1700,
    "decimal": 1700,
    "uuid": 2950,
    "jsonb": 3802,
    "serial": 23,
    "bigserial": 20,
    "vector": 16385,
}

# Array type OIDs: element_type_oid -> array_type_oid
ARRAY_TYPE_OIDS: dict[int, int] = {
    16: 1000,  # bool[]
    17: 1001,  # bytea[]
    20: 1016,  # int8[]
    21: 1005,  # int2[]
    23: 1007,  # int4[]
    25: 1009,  # text[]
    26: 1028,  # oid[]
    114: 199,  # json[]
    700: 1021,  # float4[]
    701: 1022,  # float8[]
    1042: 1014,  # bpchar[]
    1043: 1015,  # varchar[]
    1082: 1182,  # date[]
    1083: 1183,  # time[]
    1114: 1115,  # timestamp[]
    1184: 1185,  # timestamptz[]
    1700: 1231,  # numeric[]
    2950: 2951,  # uuid[]
    3802: 3807,  # jsonb[]
}

# Schema OIDs
SCHEMA_OIDS: dict[str, int] = {
    "pg_catalog": 11,
    "public": 2200,
    "information_schema": 13182,
    "pg_toast": 99,
}

# Database OID
DATABASE_OID = 1

# Superuser role OID
ROLE_OID = 10

# Access method OIDs
AM_BTREE = 403
AM_HASH = 405
AM_GIST = 783
AM_GIN = 2742
AM_BRIN = 3580
AM_HEAP = 2
AM_HNSW = 16386
AM_IVF = 16387

# pg_class OIDs for system catalogs (needed for pg_description.classoid)
CLASS_PG_CLASS = 1259
CLASS_PG_TYPE = 1247
CLASS_PG_NAMESPACE = 2615
CLASS_PG_CONSTRAINT = 2606
CLASS_PG_INDEX = 2610
CLASS_PG_ATTRDEF = 2604
CLASS_PG_AM = 2601
CLASS_PG_PROC = 1255

# Canonical type name mapping (UQA type_name -> PostgreSQL canonical name)
CANONICAL_TYPE_NAMES: dict[str, str] = {
    "int": "integer",
    "int2": "smallint",
    "int4": "integer",
    "int8": "bigint",
    "float": "real",
    "float4": "real",
    "float8": "double precision",
    "bool": "boolean",
    "serial": "integer",
    "bigserial": "bigint",
    "decimal": "numeric",
    "char": "character",
    "character varying": "character varying",
    "varchar": "character varying",
    "name": "name",
    "timestamp without time zone": "timestamp without time zone",
    "timestamp with time zone": "timestamp with time zone",
}

# Type length in bytes (-1 = variable, -2 = null-terminated C string)
TYPE_LENGTHS: dict[int, int] = {
    16: 1,  # bool
    17: -1,  # bytea
    19: 64,  # name
    20: 8,  # int8
    21: 2,  # int2
    23: 4,  # int4
    25: -1,  # text
    26: 4,  # oid
    114: -1,  # json
    142: -1,  # xml
    600: 16,  # point
    700: 4,  # float4
    701: 8,  # float8
    1042: -1,  # bpchar
    1043: -1,  # varchar
    1082: 4,  # date
    1083: 8,  # time
    1114: 8,  # timestamp
    1184: 8,  # timestamptz
    1186: 16,  # interval
    1700: -1,  # numeric
    2950: 16,  # uuid
    3802: -1,  # jsonb
    16385: -1,  # vector
}

# Type category (single character, PostgreSQL convention)
TYPE_CATEGORIES: dict[int, str] = {
    16: "B",  # Boolean
    17: "U",  # User-defined (bytea)
    19: "S",  # String
    20: "N",  # Numeric
    21: "N",  # Numeric
    23: "N",  # Numeric
    25: "S",  # String
    26: "N",  # Numeric (oid)
    114: "U",  # User-defined (json)
    142: "U",  # User-defined (xml)
    600: "G",  # Geometric
    700: "N",  # Numeric
    701: "N",  # Numeric
    1042: "S",  # String
    1043: "S",  # String
    1082: "D",  # Date/Time
    1083: "D",  # Date/Time
    1114: "D",  # Date/Time
    1184: "D",  # Date/Time
    1186: "T",  # Timespan
    1700: "N",  # Numeric
    2950: "U",  # User-defined (uuid)
    3802: "U",  # User-defined (jsonb)
    16385: "U",  # User-defined (vector)
}

# Type by-value flag (passed by value vs by reference)
TYPE_BYVAL: dict[int, bool] = {
    16: True,  # bool
    21: True,  # int2
    23: True,  # int4
    26: True,  # oid
    700: True,  # float4
}

# Type alignment
TYPE_ALIGN: dict[int, str] = {
    16: "c",  # char alignment
    17: "i",  # int alignment
    19: "c",  # char
    20: "d",  # double
    21: "s",  # short
    23: "i",  # int
    25: "i",  # int
    26: "i",  # int
    700: "i",  # int
    701: "d",  # double
    1042: "i",  # int
    1043: "i",  # int
    1082: "i",  # int
    1083: "d",  # double
    1114: "d",  # double
    1184: "d",  # double
    1186: "d",  # double
    1700: "i",  # int
    2950: "c",  # char
    3802: "i",  # int
}

# Type storage strategy
TYPE_STORAGE: dict[int, str] = {
    16: "p",  # plain
    17: "x",  # extended
    19: "p",  # plain
    20: "p",  # plain
    21: "p",  # plain
    23: "p",  # plain
    25: "x",  # extended
    26: "p",  # plain
    114: "x",  # extended
    700: "p",  # plain
    701: "p",  # plain
    1042: "x",  # extended
    1043: "x",  # extended
    1082: "p",  # plain
    1114: "p",  # plain
    1184: "p",  # plain
    1700: "m",  # main
    2950: "p",  # plain
    3802: "x",  # extended
}


def type_oid(type_name: str) -> int:
    """Resolve a UQA type name to its PostgreSQL OID."""
    # Handle array types (e.g. "text[]", "integer[]")
    if type_name.endswith("[]"):
        base = type_name[:-2]
        base_oid = TYPE_OIDS.get(base, 25)  # default to text
        return ARRAY_TYPE_OIDS.get(base_oid, 1009)  # default to text[]
    return TYPE_OIDS.get(type_name, 25)  # default to text


def canonical_type_name(type_name: str) -> str:
    """Map a UQA type name to the PostgreSQL canonical display name."""
    if type_name.endswith("[]"):
        base = type_name[:-2]
        base_canonical = CANONICAL_TYPE_NAMES.get(base, base)
        return f"{base_canonical}[]"
    return CANONICAL_TYPE_NAMES.get(type_name, type_name)


def type_length(type_name: str) -> int:
    """Return the storage length in bytes for a type (-1 = variable)."""
    oid = type_oid(type_name)
    return TYPE_LENGTHS.get(oid, -1)


def numeric_precision(type_name: str) -> int | None:
    """Return numeric precision for numeric types, None otherwise."""
    oid = type_oid(type_name)
    # PostgreSQL numeric precision values
    precisions: dict[int, int] = {
        21: 16,  # int2
        23: 32,  # int4
        20: 64,  # int8
        700: 24,  # float4
        701: 53,  # float8
    }
    return precisions.get(oid)


def numeric_scale(type_name: str) -> int | None:
    """Return numeric scale for integer types, None for others."""
    oid = type_oid(type_name)
    if oid in (21, 23, 20):
        return 0
    return None


def numeric_precision_radix(type_name: str) -> int | None:
    """Return precision radix (2 for floating, 10 for exact numeric)."""
    oid = type_oid(type_name)
    if oid in (21, 23, 20, 1700):
        return 10
    if oid in (700, 701):
        return 2
    return None


def character_maximum_length(type_name: str) -> int | None:
    """Return max character length for string types, None for others."""
    # UQA text types are unbounded
    return None


def character_octet_length(type_name: str) -> int | None:
    """Return max byte length for string types, None for others."""
    oid = type_oid(type_name)
    if oid in (25, 1042, 1043):
        return 1073741824  # 1 GB (PostgreSQL default)
    return None


# ======================================================================
# OID allocator
# ======================================================================


class OIDAllocator:
    """Assigns consistent PostgreSQL OIDs to all engine objects.

    Created once per query compilation to ensure OID consistency
    across pg_catalog and information_schema views.  Tables, views,
    indexes, sequences, constraints, and foreign tables all receive
    deterministic OIDs (sorted by name for reproducibility).
    """

    def __init__(self, engine: Engine) -> None:
        self._map: dict[tuple[str, str], int] = {}
        self._next = 16384
        self._build(engine)

    def _alloc(self) -> int:
        oid = self._next
        self._next += 1
        return oid

    def _build(self, engine: Engine) -> None:
        # -- Tables -------------------------------------------------------
        for name in sorted(engine._tables):
            self._map[("table", name)] = self._alloc()
            # Each table has an implicit composite type
            self._map[("table_type", name)] = self._alloc()
            # Each table has an implicit TOAST table OID (placeholder)
            self._map[("toast", name)] = self._alloc()

        # -- Views --------------------------------------------------------
        for name in sorted(engine._views):
            self._map[("view", name)] = self._alloc()

        # -- Sequences ----------------------------------------------------
        for name in sorted(engine._sequences):
            self._map[("sequence", name)] = self._alloc()

        # -- Foreign tables -----------------------------------------------
        for name in sorted(engine._foreign_tables):
            self._map[("foreign_table", name)] = self._alloc()

        # -- Foreign servers ----------------------------------------------
        for name in sorted(engine._foreign_servers):
            self._map[("foreign_server", name)] = self._alloc()

        # -- FDW wrappers -------------------------------------------------
        fdw_types: set[str] = set()
        for srv in engine._foreign_servers.values():
            fdw_types.add(srv.fdw_type)
        for fdw_type in sorted(fdw_types):
            self._map[("fdw", fdw_type)] = self._alloc()

        # -- Indexes ------------------------------------------------------
        index_manager = getattr(engine, "_index_manager", None)
        if index_manager is not None:
            indexes = getattr(index_manager, "_indexes", {})
            for name in sorted(indexes):
                self._map[("index", name)] = self._alloc()

        # -- Constraints --------------------------------------------------
        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            if table.primary_key:
                self._map[("constraint", f"{tname}_pkey")] = self._alloc()
                # PK also has a backing index
                self._map[("index", f"{tname}_pkey")] = self._alloc()
            for cname, cdef in table.columns.items():
                if cdef.unique and not cdef.primary_key:
                    self._map[("constraint", f"{tname}_{cname}_key")] = self._alloc()
                    self._map[("index", f"{tname}_{cname}_key")] = self._alloc()
            for fk in table.foreign_keys:
                self._map[("constraint", f"{tname}_{fk.column}_fkey")] = self._alloc()
            for check_name, _ in table.check_constraints:
                self._map[("constraint", f"{tname}_{check_name}_check")] = self._alloc()

    def get(self, category: str, name: str) -> int | None:
        """Look up an OID, returning None if not found."""
        return self._map.get((category, name))

    def get_or_alloc(self, category: str, name: str) -> int:
        """Look up or allocate a new OID."""
        key = (category, name)
        if key not in self._map:
            self._map[key] = self._alloc()
        return self._map[key]

    def relation_oid(self, name: str, engine: Engine) -> int | None:
        """Resolve a relation name to its OID across all relation types."""
        for category in ("table", "view", "foreign_table", "sequence"):
            oid = self._map.get((category, name))
            if oid is not None:
                return oid
        return None

    def all_by_category(self, category: str) -> dict[str, int]:
        """Return {name: oid} for all objects in a category."""
        return {name: oid for (cat, name), oid in self._map.items() if cat == category}
