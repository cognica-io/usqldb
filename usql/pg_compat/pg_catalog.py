#
# usql -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL 17 pg_catalog table provider.

Generates virtual rows for pg_catalog system tables by inspecting
UQA Engine state.  Each public method returns (columns, rows).

The pg_catalog tables are the real system catalog in PostgreSQL.
The information_schema views are SQL-standard wrappers built on top
of pg_catalog.  Tools like psql, SQLAlchemy, DBeaver query pg_catalog
directly for features beyond the SQL standard.

Reference: PostgreSQL 17 documentation, Chapter 53 -- System Catalogs
https://www.postgresql.org/docs/17/catalogs.html
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from usql.pg_compat.oid import (
    AM_BTREE,
    AM_HASH,
    AM_HEAP,
    AM_HNSW,
    AM_IVF,
    ARRAY_TYPE_OIDS,
    DATABASE_OID,
    ROLE_OID,
    SCHEMA_OIDS,
    TYPE_ALIGN,
    TYPE_BYVAL,
    TYPE_LENGTHS,
    TYPE_STORAGE,
    OIDAllocator,
    type_oid,
)

if TYPE_CHECKING:
    from uqa.engine import Engine

_CATALOG_NAME = "uqa"
_SCHEMA = "public"
_OWNER = "uqa"
_ENCODING_UTF8 = 6


class PGCatalogProvider:
    """Builds pg_catalog tables from UQA Engine metadata."""

    _TABLES: dict[str, str] = {
        "pg_namespace": "_build_pg_namespace",
        "pg_class": "_build_pg_class",
        "pg_attribute": "_build_pg_attribute",
        "pg_type": "_build_pg_type",
        "pg_constraint": "_build_pg_constraint",
        "pg_index": "_build_pg_index",
        "pg_attrdef": "_build_pg_attrdef",
        "pg_am": "_build_pg_am",
        "pg_database": "_build_pg_database",
        "pg_roles": "_build_pg_roles",
        "pg_user": "_build_pg_user",
        "pg_tables": "_build_pg_tables",
        "pg_views": "_build_pg_views",
        "pg_indexes": "_build_pg_indexes",
        "pg_matviews": "_build_pg_matviews",
        "pg_sequences": "_build_pg_sequences",
        "pg_settings": "_build_pg_settings",
        "pg_foreign_server": "_build_pg_foreign_server",
        "pg_foreign_table": "_build_pg_foreign_table",
        "pg_foreign_data_wrapper": "_build_pg_foreign_data_wrapper",
        "pg_description": "_build_pg_description",
        "pg_depend": "_build_pg_depend",
        "pg_stat_user_tables": "_build_pg_stat_user_tables",
        "pg_stat_user_indexes": "_build_pg_stat_user_indexes",
        "pg_stat_activity": "_build_pg_stat_activity",
        "pg_proc": "_build_pg_proc",
        "pg_extension": "_build_pg_extension",
        "pg_collation": "_build_pg_collation",
        "pg_enum": "_build_pg_enum",
        "pg_inherits": "_build_pg_inherits",
        "pg_trigger": "_build_pg_trigger",
        "pg_statio_user_tables": "_build_pg_statio_user_tables",
        "pg_auth_members": "_build_pg_auth_members",
        "pg_available_extensions": "_build_pg_available_extensions",
        "pg_stat_all_tables": "_build_pg_stat_user_tables",
    }

    @classmethod
    def supported_tables(cls) -> list[str]:
        """Return all supported pg_catalog table names."""
        return list(cls._TABLES.keys())

    @classmethod
    def build(
        cls,
        table_name: str,
        engine: Engine,
        oids: OIDAllocator,
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Build a named pg_catalog table.

        Returns (column_names, rows) or raises ValueError.
        """
        method_name = cls._TABLES.get(table_name)
        if method_name is None:
            raise ValueError(f"Unknown pg_catalog table: '{table_name}'")
        method = getattr(cls, method_name)
        return method(engine, oids)

    # ==================================================================
    # pg_namespace -- schemas
    # ==================================================================

    @staticmethod
    def _build_pg_namespace(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = ["oid", "nspname", "nspowner", "nspacl"]
        rows = [
            {
                "oid": SCHEMA_OIDS["pg_catalog"],
                "nspname": "pg_catalog",
                "nspowner": ROLE_OID,
                "nspacl": None,
            },
            {
                "oid": SCHEMA_OIDS["public"],
                "nspname": "public",
                "nspowner": ROLE_OID,
                "nspacl": None,
            },
            {
                "oid": SCHEMA_OIDS["information_schema"],
                "nspname": "information_schema",
                "nspowner": ROLE_OID,
                "nspacl": None,
            },
        ]
        return columns, rows

    # ==================================================================
    # pg_class -- all relations (tables, views, indexes, sequences, etc.)
    # ==================================================================

    @staticmethod
    def _build_pg_class(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "relname",
            "relnamespace",
            "reltype",
            "reloftype",
            "relowner",
            "relam",
            "relfilenode",
            "reltablespace",
            "relpages",
            "reltuples",
            "relallvisible",
            "reltoastrelid",
            "relhasindex",
            "relisshared",
            "relpersistence",
            "relkind",
            "relnatts",
            "relchecks",
            "relhasrules",
            "relhastriggers",
            "relhassubclass",
            "relrowsecurity",
            "relforcerowsecurity",
            "relispopulated",
            "relreplident",
            "relispartition",
            "relrewrite",
            "relfrozenxid",
            "relminmxid",
            "relacl",
            "reloptions",
            "relpartbound",
        ]
        rows: list[dict[str, Any]] = []
        ns_public = SCHEMA_OIDS["public"]

        # -- Regular tables -----------------------------------------------
        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            table_oid = oids.get("table", tname) or 0
            type_oid_val = oids.get("table_type", tname) or 0
            has_index = bool(table.primary_key) or any(
                c.unique for c in table.columns.values()
            )
            n_checks = len(table.check_constraints)
            reltuples = float(table.row_count)

            rows.append(
                _pg_class_row(
                    oid=table_oid,
                    relname=tname,
                    relnamespace=ns_public,
                    reltype=type_oid_val,
                    relam=AM_HEAP,
                    reltuples=reltuples,
                    relhasindex=has_index,
                    relkind="r",
                    relnatts=len(table.columns),
                    relchecks=n_checks,
                )
            )

        # -- Views --------------------------------------------------------
        for vname in sorted(engine._views):
            view_oid = oids.get("view", vname) or 0
            rows.append(
                _pg_class_row(
                    oid=view_oid,
                    relname=vname,
                    relnamespace=ns_public,
                    relkind="v",
                    relhasrules=True,
                )
            )

        # -- Sequences ----------------------------------------------------
        for sname in sorted(engine._sequences):
            seq_oid = oids.get("sequence", sname) or 0
            rows.append(
                _pg_class_row(
                    oid=seq_oid,
                    relname=sname,
                    relnamespace=ns_public,
                    relkind="S",
                    relnatts=3,
                )
            )

        # -- Foreign tables -----------------------------------------------
        for ftname in sorted(engine._foreign_tables):
            ft_oid = oids.get("foreign_table", ftname) or 0
            ft = engine._foreign_tables[ftname]
            rows.append(
                _pg_class_row(
                    oid=ft_oid,
                    relname=ftname,
                    relnamespace=ns_public,
                    relkind="f",
                    relnatts=len(ft.columns),
                )
            )

        # -- Indexes (explicit) -------------------------------------------
        index_manager = getattr(engine, "_index_manager", None)
        if index_manager is not None:
            for idx_name, idx_obj in sorted(
                getattr(index_manager, "_indexes", {}).items()
            ):
                idx_oid = oids.get("index", idx_name) or 0
                idx_def = idx_obj.index_def
                table_oid = oids.get("table", idx_def.table_name) or 0
                rows.append(
                    _pg_class_row(
                        oid=idx_oid,
                        relname=idx_name,
                        relnamespace=ns_public,
                        relam=AM_BTREE,
                        relkind="i",
                        relnatts=len(idx_def.columns),
                    )
                )

        # -- Implicit PK/UNIQUE indexes -----------------------------------
        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            if table.primary_key:
                pk_idx_name = f"{tname}_pkey"
                pk_idx_oid = oids.get("index", pk_idx_name) or 0
                rows.append(
                    _pg_class_row(
                        oid=pk_idx_oid,
                        relname=pk_idx_name,
                        relnamespace=ns_public,
                        relam=AM_BTREE,
                        relkind="i",
                        relnatts=1,
                    )
                )
            for cname, cdef in table.columns.items():
                if cdef.unique and not cdef.primary_key:
                    uq_idx_name = f"{tname}_{cname}_key"
                    uq_idx_oid = oids.get("index", uq_idx_name) or 0
                    rows.append(
                        _pg_class_row(
                            oid=uq_idx_oid,
                            relname=uq_idx_name,
                            relnamespace=ns_public,
                            relam=AM_BTREE,
                            relkind="i",
                            relnatts=1,
                        )
                    )

        return columns, rows

    # ==================================================================
    # pg_attribute -- columns of all relations
    # ==================================================================

    @staticmethod
    def _build_pg_attribute(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "attrelid",
            "attname",
            "atttypid",
            "attstattarget",
            "attlen",
            "attnum",
            "attndims",
            "attcacheoff",
            "atttypmod",
            "attbyval",
            "attalign",
            "attstorage",
            "attcompression",
            "attnotnull",
            "atthasdef",
            "atthasmissing",
            "attidentity",
            "attgenerated",
            "attisdropped",
            "attislocal",
            "attinhcount",
            "attcollation",
            "attacl",
            "attoptions",
            "attfdwoptions",
            "attmissingval",
        ]
        rows: list[dict[str, Any]] = []

        # System columns present in every table (hidden, negative attnum)
        system_cols = [
            ("tableoid", 26, 4, -6),
            ("cmax", 29, 4, -5),
            ("xmax", 28, 4, -4),
            ("cmin", 29, 4, -3),
            ("xmin", 28, 4, -2),
            ("ctid", 27, 6, -1),
        ]

        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            table_oid = oids.get("table", tname) or 0

            # User columns
            for attnum, (cname, cdef) in enumerate(table.columns.items(), 1):
                col_type_oid = type_oid(cdef.type_name)
                attlen = TYPE_LENGTHS.get(col_type_oid, -1)
                byval = TYPE_BYVAL.get(col_type_oid, False)
                align = TYPE_ALIGN.get(col_type_oid, "i")
                storage = TYPE_STORAGE.get(col_type_oid, "p")
                ndims = 1 if cdef.type_name.endswith("[]") else 0
                has_default = cdef.default is not None or cdef.auto_increment
                identity = "d" if cdef.auto_increment else ""

                # String types use default collation
                collation = 100 if col_type_oid in (25, 1042, 1043) else 0

                rows.append(
                    {
                        "attrelid": table_oid,
                        "attname": cname,
                        "atttypid": col_type_oid,
                        "attstattarget": -1,
                        "attlen": attlen,
                        "attnum": attnum,
                        "attndims": ndims,
                        "attcacheoff": -1,
                        "atttypmod": -1,
                        "attbyval": byval,
                        "attalign": align,
                        "attstorage": storage,
                        "attcompression": "",
                        "attnotnull": cdef.not_null or cdef.primary_key,
                        "atthasdef": has_default,
                        "atthasmissing": False,
                        "attidentity": identity,
                        "attgenerated": "",
                        "attisdropped": False,
                        "attislocal": True,
                        "attinhcount": 0,
                        "attcollation": collation,
                        "attacl": None,
                        "attoptions": None,
                        "attfdwoptions": None,
                        "attmissingval": None,
                    }
                )

            # System columns
            for sys_name, sys_type, sys_len, sys_num in system_cols:
                rows.append(
                    {
                        "attrelid": table_oid,
                        "attname": sys_name,
                        "atttypid": sys_type,
                        "attstattarget": 0,
                        "attlen": sys_len,
                        "attnum": sys_num,
                        "attndims": 0,
                        "attcacheoff": -1,
                        "atttypmod": -1,
                        "attbyval": True,
                        "attalign": "i" if sys_len == 4 else "s",
                        "attstorage": "p",
                        "attcompression": "",
                        "attnotnull": True,
                        "atthasdef": False,
                        "atthasmissing": False,
                        "attidentity": "",
                        "attgenerated": "",
                        "attisdropped": False,
                        "attislocal": True,
                        "attinhcount": 0,
                        "attcollation": 0,
                        "attacl": None,
                        "attoptions": None,
                        "attfdwoptions": None,
                        "attmissingval": None,
                    }
                )

        # Foreign table columns
        for ftname in sorted(engine._foreign_tables):
            ft = engine._foreign_tables[ftname]
            ft_oid = oids.get("foreign_table", ftname) or 0
            for attnum, (cname, cdef) in enumerate(ft.columns.items(), 1):
                col_type_oid = type_oid(cdef.type_name)
                attlen = TYPE_LENGTHS.get(col_type_oid, -1)
                rows.append(
                    {
                        "attrelid": ft_oid,
                        "attname": cname,
                        "atttypid": col_type_oid,
                        "attstattarget": -1,
                        "attlen": attlen,
                        "attnum": attnum,
                        "attndims": 0,
                        "attcacheoff": -1,
                        "atttypmod": -1,
                        "attbyval": TYPE_BYVAL.get(col_type_oid, False),
                        "attalign": TYPE_ALIGN.get(col_type_oid, "i"),
                        "attstorage": TYPE_STORAGE.get(col_type_oid, "p"),
                        "attcompression": "",
                        "attnotnull": False,
                        "atthasdef": False,
                        "atthasmissing": False,
                        "attidentity": "",
                        "attgenerated": "",
                        "attisdropped": False,
                        "attislocal": True,
                        "attinhcount": 0,
                        "attcollation": (
                            100 if col_type_oid in (25, 1042, 1043) else 0
                        ),
                        "attacl": None,
                        "attoptions": None,
                        "attfdwoptions": None,
                        "attmissingval": None,
                    }
                )

        return columns, rows

    # ==================================================================
    # pg_type -- data types
    # ==================================================================

    @staticmethod
    def _build_pg_type(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "typname",
            "typnamespace",
            "typowner",
            "typlen",
            "typbyval",
            "typtype",
            "typcategory",
            "typispreferred",
            "typisdefined",
            "typdelim",
            "typrelid",
            "typsubscript",
            "typelem",
            "typarray",
            "typinput",
            "typoutput",
            "typreceive",
            "typsend",
            "typmodin",
            "typmodout",
            "typanalyze",
            "typalign",
            "typstorage",
            "typnotnull",
            "typbasetype",
            "typtypmod",
            "typndims",
            "typcollation",
            "typdefaultbin",
            "typdefault",
            "typacl",
        ]
        ns_pg_catalog = SCHEMA_OIDS["pg_catalog"]
        rows: list[dict[str, Any]] = []

        # All built-in base types
        base_types = [
            (16, "bool", 1, True, "B", True),
            (17, "bytea", -1, False, "U", False),
            (18, "char", 1, True, "Z", False),
            (19, "name", 64, False, "S", False),
            (20, "int8", 8, True, "N", False),
            (21, "int2", 2, True, "N", False),
            (23, "int4", 4, True, "N", False),
            (25, "text", -1, False, "S", True),
            (26, "oid", 4, True, "N", False),
            (27, "tid", 6, False, "U", False),
            (28, "xid", 4, True, "U", False),
            (29, "cid", 4, True, "U", False),
            (114, "json", -1, False, "U", False),
            (142, "xml", -1, False, "U", False),
            (600, "point", 16, False, "G", False),
            (700, "float4", 4, True, "N", False),
            (701, "float8", 8, True, "N", True),
            (1042, "bpchar", -1, False, "S", False),
            (1043, "varchar", -1, False, "S", False),
            (1082, "date", 4, True, "D", False),
            (1083, "time", 8, True, "D", False),
            (1114, "timestamp", 8, True, "D", False),
            (1184, "timestamptz", 8, True, "D", True),
            (1186, "interval", 16, False, "T", True),
            (1700, "numeric", -1, False, "N", False),
            (2205, "regclass", 4, True, "N", False),
            (2249, "record", -1, False, "P", False),
            (2278, "void", 4, True, "P", False),
            (2950, "uuid", 16, False, "U", False),
            (3802, "jsonb", -1, False, "U", False),
            (16385, "vector", -1, False, "U", False),
        ]

        for type_oid_val, typname, typlen, byval, cat, preferred in base_types:
            array_oid = ARRAY_TYPE_OIDS.get(type_oid_val, 0)
            align = TYPE_ALIGN.get(type_oid_val, "i")
            storage = TYPE_STORAGE.get(type_oid_val, "p")
            collation = 100 if cat == "S" else 0

            rows.append(
                {
                    "oid": type_oid_val,
                    "typname": typname,
                    "typnamespace": ns_pg_catalog,
                    "typowner": ROLE_OID,
                    "typlen": typlen,
                    "typbyval": byval,
                    "typtype": "b",
                    "typcategory": cat,
                    "typispreferred": preferred,
                    "typisdefined": True,
                    "typdelim": ",",
                    "typrelid": 0,
                    "typsubscript": "",
                    "typelem": 0,
                    "typarray": array_oid,
                    "typinput": f"{typname}in",
                    "typoutput": f"{typname}out",
                    "typreceive": f"{typname}recv",
                    "typsend": f"{typname}send",
                    "typmodin": "",
                    "typmodout": "",
                    "typanalyze": "",
                    "typalign": align,
                    "typstorage": storage,
                    "typnotnull": False,
                    "typbasetype": 0,
                    "typtypmod": -1,
                    "typndims": 0,
                    "typcollation": collation,
                    "typdefaultbin": None,
                    "typdefault": None,
                    "typacl": None,
                }
            )

        # Array types
        for elem_oid, arr_oid in sorted(ARRAY_TYPE_OIDS.items()):
            # Find the element type name
            elem_name = ""
            for bt_oid, bt_name, *_ in base_types:
                if bt_oid == elem_oid:
                    elem_name = bt_name
                    break
            if not elem_name:
                continue
            rows.append(
                {
                    "oid": arr_oid,
                    "typname": f"_{elem_name}",
                    "typnamespace": ns_pg_catalog,
                    "typowner": ROLE_OID,
                    "typlen": -1,
                    "typbyval": False,
                    "typtype": "b",
                    "typcategory": "A",
                    "typispreferred": False,
                    "typisdefined": True,
                    "typdelim": ",",
                    "typrelid": 0,
                    "typsubscript": "array_subscript_handler",
                    "typelem": elem_oid,
                    "typarray": 0,
                    "typinput": "array_in",
                    "typoutput": "array_out",
                    "typreceive": "array_recv",
                    "typsend": "array_send",
                    "typmodin": "",
                    "typmodout": "",
                    "typanalyze": "",
                    "typalign": "i",
                    "typstorage": "x",
                    "typnotnull": False,
                    "typbasetype": 0,
                    "typtypmod": -1,
                    "typndims": 0,
                    "typcollation": 0,
                    "typdefaultbin": None,
                    "typdefault": None,
                    "typacl": None,
                }
            )

        # Composite types for user tables
        for tname in sorted(engine._tables):
            comp_oid = oids.get("table_type", tname) or 0
            table_oid = oids.get("table", tname) or 0
            rows.append(
                {
                    "oid": comp_oid,
                    "typname": tname,
                    "typnamespace": SCHEMA_OIDS["public"],
                    "typowner": ROLE_OID,
                    "typlen": -1,
                    "typbyval": False,
                    "typtype": "c",
                    "typcategory": "C",
                    "typispreferred": False,
                    "typisdefined": True,
                    "typdelim": ",",
                    "typrelid": table_oid,
                    "typsubscript": "",
                    "typelem": 0,
                    "typarray": 0,
                    "typinput": "record_in",
                    "typoutput": "record_out",
                    "typreceive": "record_recv",
                    "typsend": "record_send",
                    "typmodin": "",
                    "typmodout": "",
                    "typanalyze": "",
                    "typalign": "d",
                    "typstorage": "x",
                    "typnotnull": False,
                    "typbasetype": 0,
                    "typtypmod": -1,
                    "typndims": 0,
                    "typcollation": 0,
                    "typdefaultbin": None,
                    "typdefault": None,
                    "typacl": None,
                }
            )

        return columns, rows

    # ==================================================================
    # pg_constraint
    # ==================================================================

    @staticmethod
    def _build_pg_constraint(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "conname",
            "connamespace",
            "contype",
            "condeferrable",
            "condeferred",
            "convalidated",
            "conrelid",
            "contypid",
            "conindid",
            "conparentid",
            "confrelid",
            "confupdtype",
            "confdeltype",
            "confmatchtype",
            "conislocal",
            "coninhcount",
            "connoinherit",
            "conkey",
            "confkey",
            "conpfeqop",
            "conppeqop",
            "conffeqop",
            "conexclop",
            "conbin",
        ]
        rows: list[dict[str, Any]] = []
        ns_public = SCHEMA_OIDS["public"]

        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            table_oid = oids.get("table", tname) or 0
            col_nums = {cname: num for num, cname in enumerate(table.columns, 1)}

            # PRIMARY KEY
            if table.primary_key:
                con_name = f"{tname}_pkey"
                con_oid = oids.get("constraint", con_name) or 0
                idx_oid = oids.get("index", con_name) or 0
                pk_attnum = col_nums.get(table.primary_key, 1)
                rows.append(
                    {
                        "oid": con_oid,
                        "conname": con_name,
                        "connamespace": ns_public,
                        "contype": "p",
                        "condeferrable": False,
                        "condeferred": False,
                        "convalidated": True,
                        "conrelid": table_oid,
                        "contypid": 0,
                        "conindid": idx_oid,
                        "conparentid": 0,
                        "confrelid": 0,
                        "confupdtype": " ",
                        "confdeltype": " ",
                        "confmatchtype": " ",
                        "conislocal": True,
                        "coninhcount": 0,
                        "connoinherit": True,
                        "conkey": f"{{{pk_attnum}}}",
                        "confkey": None,
                        "conpfeqop": None,
                        "conppeqop": None,
                        "conffeqop": None,
                        "conexclop": None,
                        "conbin": None,
                    }
                )

            # UNIQUE constraints
            for cname, cdef in table.columns.items():
                if cdef.unique and not cdef.primary_key:
                    con_name = f"{tname}_{cname}_key"
                    con_oid = oids.get("constraint", con_name) or 0
                    idx_oid = oids.get("index", con_name) or 0
                    attnum = col_nums.get(cname, 1)
                    rows.append(
                        {
                            "oid": con_oid,
                            "conname": con_name,
                            "connamespace": ns_public,
                            "contype": "u",
                            "condeferrable": False,
                            "condeferred": False,
                            "convalidated": True,
                            "conrelid": table_oid,
                            "contypid": 0,
                            "conindid": idx_oid,
                            "conparentid": 0,
                            "confrelid": 0,
                            "confupdtype": " ",
                            "confdeltype": " ",
                            "confmatchtype": " ",
                            "conislocal": True,
                            "coninhcount": 0,
                            "connoinherit": True,
                            "conkey": f"{{{attnum}}}",
                            "confkey": None,
                            "conpfeqop": None,
                            "conppeqop": None,
                            "conffeqop": None,
                            "conexclop": None,
                            "conbin": None,
                        }
                    )

            # FOREIGN KEY constraints
            for fk in table.foreign_keys:
                con_name = f"{tname}_{fk.column}_fkey"
                con_oid = oids.get("constraint", con_name) or 0
                fk_attnum = col_nums.get(fk.column, 1)
                ref_table_oid = oids.get("table", fk.ref_table) or 0
                ref_table_obj = engine._tables.get(fk.ref_table)
                ref_attnum = 1
                if ref_table_obj is not None:
                    ref_col_nums = {
                        cn: n for n, cn in enumerate(ref_table_obj.columns, 1)
                    }
                    ref_attnum = ref_col_nums.get(fk.ref_column, 1)

                rows.append(
                    {
                        "oid": con_oid,
                        "conname": con_name,
                        "connamespace": ns_public,
                        "contype": "f",
                        "condeferrable": False,
                        "condeferred": False,
                        "convalidated": True,
                        "conrelid": table_oid,
                        "contypid": 0,
                        "conindid": 0,
                        "conparentid": 0,
                        "confrelid": ref_table_oid,
                        "confupdtype": "a",
                        "confdeltype": "a",
                        "confmatchtype": "s",
                        "conislocal": True,
                        "coninhcount": 0,
                        "connoinherit": True,
                        "conkey": f"{{{fk_attnum}}}",
                        "confkey": f"{{{ref_attnum}}}",
                        "conpfeqop": None,
                        "conppeqop": None,
                        "conffeqop": None,
                        "conexclop": None,
                        "conbin": None,
                    }
                )

            # CHECK constraints
            for check_name, _ in table.check_constraints:
                con_name = f"{tname}_{check_name}_check"
                con_oid = oids.get("constraint", con_name) or 0
                rows.append(
                    {
                        "oid": con_oid,
                        "conname": con_name,
                        "connamespace": ns_public,
                        "contype": "c",
                        "condeferrable": False,
                        "condeferred": False,
                        "convalidated": True,
                        "conrelid": table_oid,
                        "contypid": 0,
                        "conindid": 0,
                        "conparentid": 0,
                        "confrelid": 0,
                        "confupdtype": " ",
                        "confdeltype": " ",
                        "confmatchtype": " ",
                        "conislocal": True,
                        "coninhcount": 0,
                        "connoinherit": True,
                        "conkey": None,
                        "confkey": None,
                        "conpfeqop": None,
                        "conppeqop": None,
                        "conffeqop": None,
                        "conexclop": None,
                        "conbin": None,
                    }
                )

        return columns, rows

    # ==================================================================
    # pg_index
    # ==================================================================

    @staticmethod
    def _build_pg_index(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "indexrelid",
            "indrelid",
            "indnatts",
            "indnkeyatts",
            "indisunique",
            "indisprimary",
            "indisexclusion",
            "indimmediate",
            "indisclustered",
            "indisvalid",
            "indcheckxmin",
            "indisready",
            "indislive",
            "indisreplident",
            "indkey",
            "indcollation",
            "indclass",
            "indoption",
            "indexprs",
            "indpred",
        ]
        rows: list[dict[str, Any]] = []

        # Explicit indexes from IndexManager
        index_manager = getattr(engine, "_index_manager", None)
        if index_manager is not None:
            for idx_name, idx_obj in sorted(
                getattr(index_manager, "_indexes", {}).items()
            ):
                idx_oid = oids.get("index", idx_name) or 0
                idx_def = idx_obj.index_def
                table_oid = oids.get("table", idx_def.table_name) or 0
                table_obj = engine._tables.get(idx_def.table_name)
                n_atts = len(idx_def.columns)

                indkey_parts = []
                if table_obj is not None:
                    col_nums = {cn: n for n, cn in enumerate(table_obj.columns, 1)}
                    for col in idx_def.columns:
                        indkey_parts.append(str(col_nums.get(col, 0)))
                indkey = " ".join(indkey_parts) if indkey_parts else "0"

                is_unique = getattr(idx_def, "unique", False)

                rows.append(
                    {
                        "indexrelid": idx_oid,
                        "indrelid": table_oid,
                        "indnatts": n_atts,
                        "indnkeyatts": n_atts,
                        "indisunique": is_unique,
                        "indisprimary": False,
                        "indisexclusion": False,
                        "indimmediate": True,
                        "indisclustered": False,
                        "indisvalid": True,
                        "indcheckxmin": False,
                        "indisready": True,
                        "indislive": True,
                        "indisreplident": False,
                        "indkey": indkey,
                        "indcollation": "",
                        "indclass": "",
                        "indoption": "",
                        "indexprs": None,
                        "indpred": None,
                    }
                )

        # Implicit PK/UNIQUE indexes
        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            table_oid = oids.get("table", tname) or 0
            col_nums = {cn: n for n, cn in enumerate(table.columns, 1)}

            if table.primary_key:
                pk_idx_name = f"{tname}_pkey"
                pk_idx_oid = oids.get("index", pk_idx_name) or 0
                pk_attnum = col_nums.get(table.primary_key, 0)
                rows.append(
                    {
                        "indexrelid": pk_idx_oid,
                        "indrelid": table_oid,
                        "indnatts": 1,
                        "indnkeyatts": 1,
                        "indisunique": True,
                        "indisprimary": True,
                        "indisexclusion": False,
                        "indimmediate": True,
                        "indisclustered": False,
                        "indisvalid": True,
                        "indcheckxmin": False,
                        "indisready": True,
                        "indislive": True,
                        "indisreplident": False,
                        "indkey": str(pk_attnum),
                        "indcollation": "",
                        "indclass": "",
                        "indoption": "",
                        "indexprs": None,
                        "indpred": None,
                    }
                )

            for cname, cdef in table.columns.items():
                if cdef.unique and not cdef.primary_key:
                    uq_idx_name = f"{tname}_{cname}_key"
                    uq_idx_oid = oids.get("index", uq_idx_name) or 0
                    attnum = col_nums.get(cname, 0)
                    rows.append(
                        {
                            "indexrelid": uq_idx_oid,
                            "indrelid": table_oid,
                            "indnatts": 1,
                            "indnkeyatts": 1,
                            "indisunique": True,
                            "indisprimary": False,
                            "indisexclusion": False,
                            "indimmediate": True,
                            "indisclustered": False,
                            "indisvalid": True,
                            "indcheckxmin": False,
                            "indisready": True,
                            "indislive": True,
                            "indisreplident": False,
                            "indkey": str(attnum),
                            "indcollation": "",
                            "indclass": "",
                            "indoption": "",
                            "indexprs": None,
                            "indpred": None,
                        }
                    )

        return columns, rows

    # ==================================================================
    # pg_attrdef -- column defaults
    # ==================================================================

    @staticmethod
    def _build_pg_attrdef(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = ["oid", "adrelid", "adnum", "adbin"]
        rows: list[dict[str, Any]] = []

        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            table_oid = oids.get("table", tname) or 0
            for attnum, (cname, cdef) in enumerate(table.columns.items(), 1):
                if cdef.default is not None or cdef.auto_increment:
                    def_oid = oids.get_or_alloc("attrdef", f"{tname}.{cname}")
                    if cdef.auto_increment:
                        adbin = f"nextval('{tname}_{cname}_seq'::regclass)"
                    else:
                        adbin = str(cdef.default)
                    rows.append(
                        {
                            "oid": def_oid,
                            "adrelid": table_oid,
                            "adnum": attnum,
                            "adbin": adbin,
                        }
                    )

        return columns, rows

    # ==================================================================
    # pg_am -- access methods
    # ==================================================================

    @staticmethod
    def _build_pg_am(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = ["oid", "amname", "amhandler", "amtype"]
        rows = [
            {
                "oid": AM_HEAP,
                "amname": "heap",
                "amhandler": "heap_tableam_handler",
                "amtype": "t",
            },
            {
                "oid": AM_BTREE,
                "amname": "btree",
                "amhandler": "bthandler",
                "amtype": "i",
            },
            {
                "oid": AM_HASH,
                "amname": "hash",
                "amhandler": "hashhandler",
                "amtype": "i",
            },
            {
                "oid": AM_HNSW,
                "amname": "hnsw",
                "amhandler": "hnsw_handler",
                "amtype": "i",
            },
            {
                "oid": AM_IVF,
                "amname": "ivf",
                "amhandler": "ivf_handler",
                "amtype": "i",
            },
        ]
        return columns, rows

    # ==================================================================
    # pg_database
    # ==================================================================

    @staticmethod
    def _build_pg_database(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "datname",
            "datdba",
            "encoding",
            "datlocprovider",
            "datistemplate",
            "datallowconn",
            "datconnlimit",
            "datfrozenxid",
            "datminmxid",
            "dattablespace",
            "datcollate",
            "datctype",
            "datlocale",
            "datcollversion",
            "datacl",
        ]
        rows = [
            {
                "oid": DATABASE_OID,
                "datname": _CATALOG_NAME,
                "datdba": ROLE_OID,
                "encoding": _ENCODING_UTF8,
                "datlocprovider": "c",
                "datistemplate": False,
                "datallowconn": True,
                "datconnlimit": -1,
                "datfrozenxid": 0,
                "datminmxid": 1,
                "dattablespace": 1663,
                "datcollate": "en_US.UTF-8",
                "datctype": "en_US.UTF-8",
                "datlocale": None,
                "datcollversion": None,
                "datacl": None,
            }
        ]
        return columns, rows

    # ==================================================================
    # pg_roles
    # ==================================================================

    @staticmethod
    def _build_pg_roles(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "rolname",
            "rolsuper",
            "rolinherit",
            "rolcreaterole",
            "rolcreatedb",
            "rolcanlogin",
            "rolreplication",
            "rolconnlimit",
            "rolpassword",
            "rolvaliduntil",
            "rolbypassrls",
            "rolconfig",
        ]
        rows = [
            {
                "oid": ROLE_OID,
                "rolname": _OWNER,
                "rolsuper": True,
                "rolinherit": True,
                "rolcreaterole": True,
                "rolcreatedb": True,
                "rolcanlogin": True,
                "rolreplication": True,
                "rolconnlimit": -1,
                "rolpassword": None,
                "rolvaliduntil": None,
                "rolbypassrls": True,
                "rolconfig": None,
            }
        ]
        return columns, rows

    # ==================================================================
    # pg_user -- simplified view over pg_roles
    # ==================================================================

    @staticmethod
    def _build_pg_user(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "usename",
            "usesysid",
            "usecreatedb",
            "usesuper",
            "userepl",
            "usebypassrls",
            "passwd",
            "valuntil",
            "useconfig",
        ]
        rows = [
            {
                "usename": _OWNER,
                "usesysid": ROLE_OID,
                "usecreatedb": True,
                "usesuper": True,
                "userepl": True,
                "usebypassrls": True,
                "passwd": None,
                "valuntil": None,
                "useconfig": None,
            }
        ]
        return columns, rows

    # ==================================================================
    # pg_tables -- convenience view
    # ==================================================================

    @staticmethod
    def _build_pg_tables(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "schemaname",
            "tablename",
            "tableowner",
            "tablespace",
            "hasindexes",
            "hasrules",
            "hastriggers",
            "rowsecurity",
        ]
        rows: list[dict[str, Any]] = []
        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            has_idx = bool(table.primary_key) or any(
                c.unique for c in table.columns.values()
            )
            rows.append(
                {
                    "schemaname": _SCHEMA,
                    "tablename": tname,
                    "tableowner": _OWNER,
                    "tablespace": None,
                    "hasindexes": has_idx,
                    "hasrules": False,
                    "hastriggers": False,
                    "rowsecurity": False,
                }
            )
        return columns, rows

    # ==================================================================
    # pg_views -- convenience view
    # ==================================================================

    @staticmethod
    def _build_pg_views(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = ["schemaname", "viewname", "viewowner", "definition"]
        rows: list[dict[str, Any]] = []
        for vname in sorted(engine._views):
            rows.append(
                {
                    "schemaname": _SCHEMA,
                    "viewname": vname,
                    "viewowner": _OWNER,
                    "definition": "",
                }
            )
        return columns, rows

    # ==================================================================
    # pg_indexes -- convenience view
    # ==================================================================

    @staticmethod
    def _build_pg_indexes(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "schemaname",
            "tablename",
            "indexname",
            "tablespace",
            "indexdef",
        ]
        rows: list[dict[str, Any]] = []

        index_manager = getattr(engine, "_index_manager", None)
        if index_manager is not None:
            for idx_name, idx_obj in sorted(
                getattr(index_manager, "_indexes", {}).items()
            ):
                idx_def = idx_obj.index_def
                cols_str = ", ".join(idx_def.columns)
                rows.append(
                    {
                        "schemaname": _SCHEMA,
                        "tablename": idx_def.table_name,
                        "indexname": idx_name,
                        "tablespace": None,
                        "indexdef": (
                            f"CREATE INDEX {idx_name} ON "
                            f"{idx_def.table_name} ({cols_str})"
                        ),
                    }
                )

        # Implicit PK/UNIQUE indexes
        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            if table.primary_key:
                pk_name = f"{tname}_pkey"
                rows.append(
                    {
                        "schemaname": _SCHEMA,
                        "tablename": tname,
                        "indexname": pk_name,
                        "tablespace": None,
                        "indexdef": (
                            f"CREATE UNIQUE INDEX {pk_name} ON "
                            f"{tname} ({table.primary_key})"
                        ),
                    }
                )
            for cname, cdef in table.columns.items():
                if cdef.unique and not cdef.primary_key:
                    uq_name = f"{tname}_{cname}_key"
                    rows.append(
                        {
                            "schemaname": _SCHEMA,
                            "tablename": tname,
                            "indexname": uq_name,
                            "tablespace": None,
                            "indexdef": (
                                f"CREATE UNIQUE INDEX {uq_name} ON {tname} ({cname})"
                            ),
                        }
                    )

        return columns, rows

    # ==================================================================
    # pg_matviews -- materialized views (empty, UQA has none)
    # ==================================================================

    @staticmethod
    def _build_pg_matviews(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "schemaname",
            "matviewname",
            "matviewowner",
            "tablespace",
            "hasindexes",
            "ispopulated",
            "definition",
        ]
        return columns, []

    # ==================================================================
    # pg_sequences
    # ==================================================================

    @staticmethod
    def _build_pg_sequences(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "schemaname",
            "sequencename",
            "sequenceowner",
            "data_type",
            "start_value",
            "min_value",
            "max_value",
            "increment_by",
            "cycle",
            "cache_size",
            "last_value",
        ]
        rows: list[dict[str, Any]] = []
        for sname in sorted(engine._sequences):
            seq = engine._sequences[sname]
            rows.append(
                {
                    "schemaname": _SCHEMA,
                    "sequencename": sname,
                    "sequenceowner": _OWNER,
                    "data_type": "bigint",
                    "start_value": seq.get("start", 1),
                    "min_value": 1,
                    "max_value": 9223372036854775807,
                    "increment_by": seq.get("increment", 1),
                    "cycle": False,
                    "cache_size": 1,
                    "last_value": seq.get("current", seq.get("start", 1)),
                }
            )
        return columns, rows

    # ==================================================================
    # pg_settings -- runtime parameters (GUC)
    # ==================================================================

    @staticmethod
    def _build_pg_settings(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "name",
            "setting",
            "unit",
            "category",
            "short_desc",
            "extra_desc",
            "context",
            "vartype",
            "source",
            "min_val",
            "max_val",
            "enumvals",
            "boot_val",
            "reset_val",
            "sourcefile",
            "sourceline",
            "pending_restart",
        ]
        settings = [
            (
                "server_version",
                "17.0",
                None,
                "Preset Options",
                "Shows the server version.",
                None,
                "internal",
                "string",
                "default",
                None,
                None,
                None,
                "17.0",
                "17.0",
                None,
                None,
                False,
            ),
            (
                "server_version_num",
                "170000",
                None,
                "Preset Options",
                "Shows the server version as an integer.",
                None,
                "internal",
                "integer",
                "default",
                None,
                None,
                None,
                "170000",
                "170000",
                None,
                None,
                False,
            ),
            (
                "server_encoding",
                "UTF8",
                None,
                "Preset Options",
                "Shows the server encoding.",
                None,
                "internal",
                "string",
                "default",
                None,
                None,
                None,
                "UTF8",
                "UTF8",
                None,
                None,
                False,
            ),
            (
                "client_encoding",
                "UTF8",
                None,
                "Client Connection Defaults",
                "Sets the client encoding.",
                None,
                "user",
                "string",
                "default",
                None,
                None,
                None,
                "UTF8",
                "UTF8",
                None,
                None,
                False,
            ),
            (
                "lc_collate",
                "en_US.UTF-8",
                None,
                "Preset Options",
                "Shows the collation order locale.",
                None,
                "internal",
                "string",
                "default",
                None,
                None,
                None,
                "en_US.UTF-8",
                "en_US.UTF-8",
                None,
                None,
                False,
            ),
            (
                "lc_ctype",
                "en_US.UTF-8",
                None,
                "Preset Options",
                "Shows the character classification locale.",
                None,
                "internal",
                "string",
                "default",
                None,
                None,
                None,
                "en_US.UTF-8",
                "en_US.UTF-8",
                None,
                None,
                False,
            ),
            (
                "DateStyle",
                "ISO, MDY",
                None,
                "Client Connection Defaults",
                "Sets the display format for date and time.",
                None,
                "user",
                "string",
                "default",
                None,
                None,
                None,
                "ISO, MDY",
                "ISO, MDY",
                None,
                None,
                False,
            ),
            (
                "TimeZone",
                "UTC",
                None,
                "Client Connection Defaults",
                "Sets the time zone.",
                None,
                "user",
                "string",
                "default",
                None,
                None,
                None,
                "UTC",
                "UTC",
                None,
                None,
                False,
            ),
            (
                "standard_conforming_strings",
                "on",
                None,
                "Client Connection Defaults",
                "Causes strings to treat backslashes literally.",
                None,
                "user",
                "bool",
                "default",
                None,
                None,
                None,
                "on",
                "on",
                None,
                None,
                False,
            ),
            (
                "search_path",
                '"$user", public',
                None,
                "Client Connection Defaults",
                "Sets the schema search order.",
                None,
                "user",
                "string",
                "default",
                None,
                None,
                None,
                '"$user", public',
                '"$user", public',
                None,
                None,
                False,
            ),
            (
                "default_transaction_isolation",
                "read committed",
                None,
                "Client Connection Defaults",
                "Sets the default transaction isolation level.",
                None,
                "user",
                "enum",
                "default",
                None,
                None,
                "serializable,repeatable read,read committed,read uncommitted",
                "read committed",
                "read committed",
                None,
                None,
                False,
            ),
            (
                "max_connections",
                "100",
                None,
                "Connections and Authentication",
                "Sets the maximum number of concurrent connections.",
                None,
                "postmaster",
                "integer",
                "default",
                "1",
                "262143",
                None,
                "100",
                "100",
                None,
                None,
                False,
            ),
            (
                "shared_buffers",
                "16384",
                "8kB",
                "Resource Usage / Memory",
                "Sets the number of shared memory buffers.",
                None,
                "postmaster",
                "integer",
                "default",
                "16",
                "1073741823",
                None,
                "16384",
                "16384",
                None,
                None,
                False,
            ),
            (
                "work_mem",
                "4096",
                "kB",
                "Resource Usage / Memory",
                "Sets the maximum memory for query operations.",
                None,
                "user",
                "integer",
                "default",
                "64",
                "2147483647",
                None,
                "4096",
                "4096",
                None,
                None,
                False,
            ),
            (
                "is_superuser",
                "on",
                None,
                "Preset Options",
                "Shows whether the current user is a superuser.",
                None,
                "internal",
                "bool",
                "default",
                None,
                None,
                None,
                "on",
                "on",
                None,
                None,
                False,
            ),
            (
                "transaction_isolation",
                "read committed",
                None,
                "Client Connection Defaults",
                "Shows the current transaction isolation level.",
                None,
                "user",
                "string",
                "override",
                None,
                None,
                None,
                "read committed",
                "read committed",
                None,
                None,
                False,
            ),
            (
                "integer_datetimes",
                "on",
                None,
                "Preset Options",
                "Shows if datetimes are stored as 64-bit integers.",
                None,
                "internal",
                "bool",
                "default",
                None,
                None,
                None,
                "on",
                "on",
                None,
                None,
                False,
            ),
        ]
        rows: list[dict[str, Any]] = []
        for s in settings:
            rows.append(
                {
                    "name": s[0],
                    "setting": s[1],
                    "unit": s[2],
                    "category": s[3],
                    "short_desc": s[4],
                    "extra_desc": s[5],
                    "context": s[6],
                    "vartype": s[7],
                    "source": s[8],
                    "min_val": s[9],
                    "max_val": s[10],
                    "enumvals": s[11],
                    "boot_val": s[12],
                    "reset_val": s[13],
                    "sourcefile": s[14],
                    "sourceline": s[15],
                    "pending_restart": s[16],
                }
            )
        return columns, rows

    # ==================================================================
    # pg_foreign_server
    # ==================================================================

    @staticmethod
    def _build_pg_foreign_server(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "srvname",
            "srvowner",
            "srvfdw",
            "srvtype",
            "srvversion",
            "srvacl",
            "srvoptions",
        ]
        rows: list[dict[str, Any]] = []
        for sname in sorted(engine._foreign_servers):
            srv = engine._foreign_servers[sname]
            srv_oid = oids.get("foreign_server", sname) or 0
            fdw_oid = oids.get("fdw", srv.fdw_type) or 0
            opts = [f"{k}={v}" for k, v in sorted(srv.options.items())]
            rows.append(
                {
                    "oid": srv_oid,
                    "srvname": sname,
                    "srvowner": ROLE_OID,
                    "srvfdw": fdw_oid,
                    "srvtype": None,
                    "srvversion": None,
                    "srvacl": None,
                    "srvoptions": "{" + ",".join(opts) + "}" if opts else None,
                }
            )
        return columns, rows

    # ==================================================================
    # pg_foreign_table
    # ==================================================================

    @staticmethod
    def _build_pg_foreign_table(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = ["ftrelid", "ftserver", "ftoptions"]
        rows: list[dict[str, Any]] = []
        for ftname in sorted(engine._foreign_tables):
            ft = engine._foreign_tables[ftname]
            ft_oid = oids.get("foreign_table", ftname) or 0
            srv_oid = oids.get("foreign_server", ft.server_name) or 0
            opts = [f"{k}={v}" for k, v in sorted(ft.options.items())]
            rows.append(
                {
                    "ftrelid": ft_oid,
                    "ftserver": srv_oid,
                    "ftoptions": "{" + ",".join(opts) + "}" if opts else None,
                }
            )
        return columns, rows

    # ==================================================================
    # pg_foreign_data_wrapper
    # ==================================================================

    @staticmethod
    def _build_pg_foreign_data_wrapper(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "fdwname",
            "fdwowner",
            "fdwhandler",
            "fdwvalidator",
            "fdwacl",
            "fdwoptions",
        ]
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for sname in sorted(engine._foreign_servers):
            srv = engine._foreign_servers[sname]
            if srv.fdw_type in seen:
                continue
            seen.add(srv.fdw_type)
            fdw_oid = oids.get("fdw", srv.fdw_type) or 0
            rows.append(
                {
                    "oid": fdw_oid,
                    "fdwname": srv.fdw_type,
                    "fdwowner": ROLE_OID,
                    "fdwhandler": 0,
                    "fdwvalidator": 0,
                    "fdwacl": None,
                    "fdwoptions": None,
                }
            )
        return columns, rows

    # ==================================================================
    # pg_description -- object comments
    # ==================================================================

    @staticmethod
    def _build_pg_description(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = ["objoid", "classoid", "objsubid", "description"]
        # UQA does not support COMMENT ON, return empty
        return columns, []

    # ==================================================================
    # pg_depend -- object dependencies
    # ==================================================================

    @staticmethod
    def _build_pg_depend(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "classid",
            "objid",
            "objsubid",
            "refclassid",
            "refobjid",
            "refobjsubid",
            "deptype",
        ]
        # Basic dependency tracking: FK constraints depend on their
        # referenced tables.
        rows: list[dict[str, Any]] = []
        return columns, rows

    # ==================================================================
    # pg_stat_user_tables
    # ==================================================================

    @staticmethod
    def _build_pg_stat_user_tables(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "relid",
            "schemaname",
            "relname",
            "seq_scan",
            "seq_tup_read",
            "idx_scan",
            "idx_tup_fetch",
            "n_tup_ins",
            "n_tup_upd",
            "n_tup_del",
            "n_tup_hot_upd",
            "n_live_tup",
            "n_dead_tup",
            "n_mod_since_analyze",
            "n_ins_since_vacuum",
            "last_vacuum",
            "last_autovacuum",
            "last_analyze",
            "last_autoanalyze",
            "vacuum_count",
            "autovacuum_count",
            "analyze_count",
            "autoanalyze_count",
        ]
        rows: list[dict[str, Any]] = []
        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            table_oid = oids.get("table", tname) or 0
            n_live = table.row_count
            has_stats = bool(table._stats)
            rows.append(
                {
                    "relid": table_oid,
                    "schemaname": _SCHEMA,
                    "relname": tname,
                    "seq_scan": 0,
                    "seq_tup_read": 0,
                    "idx_scan": 0,
                    "idx_tup_fetch": 0,
                    "n_tup_ins": n_live,
                    "n_tup_upd": 0,
                    "n_tup_del": 0,
                    "n_tup_hot_upd": 0,
                    "n_live_tup": n_live,
                    "n_dead_tup": 0,
                    "n_mod_since_analyze": 0 if has_stats else n_live,
                    "n_ins_since_vacuum": n_live,
                    "last_vacuum": None,
                    "last_autovacuum": None,
                    "last_analyze": None,
                    "last_autoanalyze": None,
                    "vacuum_count": 0,
                    "autovacuum_count": 0,
                    "analyze_count": 1 if has_stats else 0,
                    "autoanalyze_count": 0,
                }
            )
        return columns, rows

    # ==================================================================
    # pg_stat_user_indexes
    # ==================================================================

    @staticmethod
    def _build_pg_stat_user_indexes(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "relid",
            "indexrelid",
            "schemaname",
            "relname",
            "indexrelname",
            "idx_scan",
            "idx_tup_read",
            "idx_tup_fetch",
        ]
        rows: list[dict[str, Any]] = []

        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            table_oid = oids.get("table", tname) or 0
            if table.primary_key:
                pk_name = f"{tname}_pkey"
                pk_oid = oids.get("index", pk_name) or 0
                rows.append(
                    {
                        "relid": table_oid,
                        "indexrelid": pk_oid,
                        "schemaname": _SCHEMA,
                        "relname": tname,
                        "indexrelname": pk_name,
                        "idx_scan": 0,
                        "idx_tup_read": 0,
                        "idx_tup_fetch": 0,
                    }
                )
            for cname, cdef in table.columns.items():
                if cdef.unique and not cdef.primary_key:
                    uq_name = f"{tname}_{cname}_key"
                    uq_oid = oids.get("index", uq_name) or 0
                    rows.append(
                        {
                            "relid": table_oid,
                            "indexrelid": uq_oid,
                            "schemaname": _SCHEMA,
                            "relname": tname,
                            "indexrelname": uq_name,
                            "idx_scan": 0,
                            "idx_tup_read": 0,
                            "idx_tup_fetch": 0,
                        }
                    )

        return columns, rows

    # ==================================================================
    # pg_stat_activity -- active sessions
    # ==================================================================

    @staticmethod
    def _build_pg_stat_activity(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "datid",
            "datname",
            "pid",
            "leader_pid",
            "usesysid",
            "usename",
            "application_name",
            "client_addr",
            "client_hostname",
            "client_port",
            "backend_start",
            "xact_start",
            "query_start",
            "state_change",
            "wait_event_type",
            "wait_event",
            "state",
            "backend_xid",
            "backend_xmin",
            "query_id",
            "query",
            "backend_type",
        ]
        import os

        rows = [
            {
                "datid": DATABASE_OID,
                "datname": _CATALOG_NAME,
                "pid": os.getpid(),
                "leader_pid": None,
                "usesysid": ROLE_OID,
                "usename": _OWNER,
                "application_name": "usql",
                "client_addr": None,
                "client_hostname": None,
                "client_port": -1,
                "backend_start": None,
                "xact_start": None,
                "query_start": None,
                "state_change": None,
                "wait_event_type": None,
                "wait_event": None,
                "state": "active",
                "backend_xid": None,
                "backend_xmin": None,
                "query_id": None,
                "query": "",
                "backend_type": "client backend",
            }
        ]
        return columns, rows

    # ==================================================================
    # pg_proc -- functions/procedures
    # ==================================================================

    @staticmethod
    def _build_pg_proc(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "proname",
            "pronamespace",
            "proowner",
            "prolang",
            "procost",
            "prorows",
            "provariadic",
            "prosupport",
            "prokind",
            "prosecdef",
            "proleakproof",
            "proisstrict",
            "proretset",
            "provolatile",
            "proparallel",
            "pronargs",
            "pronargdefaults",
            "prorettype",
            "proargtypes",
            "proallargtypes",
            "proargmodes",
            "proargnames",
            "proargdefaults",
            "protrftypes",
            "prosrc",
            "probin",
            "prosqlbody",
            "proconfig",
            "proacl",
        ]
        # UQA built-in extended SQL functions
        uqa_functions = [
            ("text_match", 2, 25, "25 25"),
            ("bayesian_match", 2, 25, "25 25"),
            ("knn_match", 3, 25, "25 2277 23"),
            ("traverse_match", 3, 25, "23 25 23"),
            ("fuse_log_odds", 0, 25, ""),
            ("fuse_prob_and", 0, 25, ""),
            ("fuse_prob_or", 0, 25, ""),
            ("fuse_prob_not", 1, 25, "25"),
            ("spatial_within", 4, 25, "25 600 600 701"),
        ]
        rows: list[dict[str, Any]] = []
        ns_public = SCHEMA_OIDS["public"]
        for _i, (fname, nargs, rettype, argtypes) in enumerate(uqa_functions):
            func_oid = oids.get_or_alloc("function", fname)
            rows.append(
                {
                    "oid": func_oid,
                    "proname": fname,
                    "pronamespace": ns_public,
                    "proowner": ROLE_OID,
                    "prolang": 14,  # SQL
                    "procost": 100,
                    "prorows": 0,
                    "provariadic": 0,
                    "prosupport": "",
                    "prokind": "f",
                    "prosecdef": False,
                    "proleakproof": False,
                    "proisstrict": False,
                    "proretset": False,
                    "provolatile": "v",
                    "proparallel": "u",
                    "pronargs": nargs,
                    "pronargdefaults": 0,
                    "prorettype": rettype,
                    "proargtypes": argtypes,
                    "proallargtypes": None,
                    "proargmodes": None,
                    "proargnames": None,
                    "proargdefaults": None,
                    "protrftypes": None,
                    "prosrc": fname,
                    "probin": None,
                    "prosqlbody": None,
                    "proconfig": None,
                    "proacl": None,
                }
            )
        return columns, rows

    # ==================================================================
    # pg_extension
    # ==================================================================

    @staticmethod
    def _build_pg_extension(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "extname",
            "extowner",
            "extnamespace",
            "extrelocatable",
            "extversion",
            "extconfig",
            "extcondition",
        ]
        rows = [
            {
                "oid": 13181,
                "extname": "plpgsql",
                "extowner": ROLE_OID,
                "extnamespace": SCHEMA_OIDS["pg_catalog"],
                "extrelocatable": False,
                "extversion": "1.0",
                "extconfig": None,
                "extcondition": None,
            }
        ]
        return columns, rows

    # ==================================================================
    # pg_collation
    # ==================================================================

    @staticmethod
    def _build_pg_collation(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "collname",
            "collnamespace",
            "collowner",
            "collprovider",
            "collisdeterministic",
            "collencoding",
            "collcollate",
            "collctype",
            "colliculocale",
            "collicurules",
            "collversion",
        ]
        rows = [
            {
                "oid": 100,
                "collname": "default",
                "collnamespace": SCHEMA_OIDS["pg_catalog"],
                "collowner": ROLE_OID,
                "collprovider": "d",
                "collisdeterministic": True,
                "collencoding": -1,
                "collcollate": "",
                "collctype": "",
                "colliculocale": None,
                "collicurules": None,
                "collversion": None,
            },
            {
                "oid": 950,
                "collname": "C",
                "collnamespace": SCHEMA_OIDS["pg_catalog"],
                "collowner": ROLE_OID,
                "collprovider": "c",
                "collisdeterministic": True,
                "collencoding": -1,
                "collcollate": "C",
                "collctype": "C",
                "colliculocale": None,
                "collicurules": None,
                "collversion": None,
            },
            {
                "oid": 951,
                "collname": "POSIX",
                "collnamespace": SCHEMA_OIDS["pg_catalog"],
                "collowner": ROLE_OID,
                "collprovider": "c",
                "collisdeterministic": True,
                "collencoding": -1,
                "collcollate": "POSIX",
                "collctype": "POSIX",
                "colliculocale": None,
                "collicurules": None,
                "collversion": None,
            },
        ]
        return columns, rows

    # ==================================================================
    # pg_enum (empty -- UQA has no enum types)
    # ==================================================================

    @staticmethod
    def _build_pg_enum(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = ["oid", "enumtypid", "enumsortorder", "enumlabel"]
        return columns, []

    # ==================================================================
    # pg_inherits (empty -- no inheritance)
    # ==================================================================

    @staticmethod
    def _build_pg_inherits(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = ["inhrelid", "inhparent", "inhseqno", "inhdetachpending"]
        return columns, []

    # ==================================================================
    # pg_trigger (empty -- no triggers)
    # ==================================================================

    @staticmethod
    def _build_pg_trigger(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "tgrelid",
            "tgparentid",
            "tgname",
            "tgfoid",
            "tgtype",
            "tgenabled",
            "tgisinternal",
            "tgconstrrelid",
            "tgconstrindid",
            "tgconstraint",
            "tgdeferrable",
            "tginitdeferred",
            "tgnargs",
            "tgattr",
            "tgargs",
            "tgqual",
            "tgoldtable",
            "tgnewtable",
        ]
        return columns, []

    # ==================================================================
    # pg_statio_user_tables
    # ==================================================================

    @staticmethod
    def _build_pg_statio_user_tables(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "relid",
            "schemaname",
            "relname",
            "heap_blks_read",
            "heap_blks_hit",
            "idx_blks_read",
            "idx_blks_hit",
            "toast_blks_read",
            "toast_blks_hit",
            "tidx_blks_read",
            "tidx_blks_hit",
        ]
        rows: list[dict[str, Any]] = []
        for tname in sorted(engine._tables):
            table_oid = oids.get("table", tname) or 0
            rows.append(
                {
                    "relid": table_oid,
                    "schemaname": _SCHEMA,
                    "relname": tname,
                    "heap_blks_read": 0,
                    "heap_blks_hit": 0,
                    "idx_blks_read": 0,
                    "idx_blks_hit": 0,
                    "toast_blks_read": 0,
                    "toast_blks_hit": 0,
                    "tidx_blks_read": 0,
                    "tidx_blks_hit": 0,
                }
            )
        return columns, rows

    # ==================================================================
    # pg_auth_members (empty -- single user)
    # ==================================================================

    @staticmethod
    def _build_pg_auth_members(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "oid",
            "roleid",
            "member",
            "grantor",
            "admin_option",
            "inherit_option",
            "set_option",
        ]
        return columns, []

    # ==================================================================
    # pg_available_extensions
    # ==================================================================

    @staticmethod
    def _build_pg_available_extensions(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "name",
            "default_version",
            "installed_version",
            "comment",
        ]
        rows = [
            {
                "name": "plpgsql",
                "default_version": "1.0",
                "installed_version": "1.0",
                "comment": "PL/pgSQL procedural language",
            }
        ]
        return columns, rows


# ======================================================================
# Helpers
# ======================================================================


def _pg_class_row(
    oid: int,
    relname: str,
    relnamespace: int,
    reltype: int = 0,
    reloftype: int = 0,
    relowner: int = ROLE_OID,
    relam: int = 0,
    reltuples: float = -1,
    relhasindex: bool = False,
    relkind: str = "r",
    relnatts: int = 0,
    relchecks: int = 0,
    relhasrules: bool = False,
) -> dict[str, Any]:
    """Build a pg_class row with sensible defaults."""
    return {
        "oid": oid,
        "relname": relname,
        "relnamespace": relnamespace,
        "reltype": reltype,
        "reloftype": reloftype,
        "relowner": relowner,
        "relam": relam,
        "relfilenode": oid,
        "reltablespace": 0,
        "relpages": max(1, int(reltuples / 100)) if reltuples > 0 else 0,
        "reltuples": reltuples,
        "relallvisible": 0,
        "reltoastrelid": 0,
        "relhasindex": relhasindex,
        "relisshared": False,
        "relpersistence": "p",
        "relkind": relkind,
        "relnatts": relnatts,
        "relchecks": relchecks,
        "relhasrules": relhasrules,
        "relhastriggers": False,
        "relhassubclass": False,
        "relrowsecurity": False,
        "relforcerowsecurity": False,
        "relispopulated": True,
        "relreplident": "d",
        "relispartition": False,
        "relrewrite": 0,
        "relfrozenxid": 0,
        "relminmxid": 1,
        "relacl": None,
        "reloptions": None,
        "relpartbound": None,
    }
