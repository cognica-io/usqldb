#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL 17 information_schema view provider.

Generates virtual rows for all standard information_schema views by
inspecting UQA Engine state.  Each public method returns a
(columns, rows) tuple suitable for conversion into a virtual Table.

Reference: PostgreSQL 17 documentation, Chapter 37 -- The Information Schema
https://www.postgresql.org/docs/17/information-schema.html
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from usqldb.pg_compat.oid import (
    OIDAllocator,
    canonical_type_name,
    character_maximum_length,
    character_octet_length,
    numeric_precision,
    numeric_precision_radix,
    numeric_scale,
)

if TYPE_CHECKING:
    from uqa.engine import Engine

# Database name used as table_catalog
_CATALOG = "uqa"

# Default schema for user objects
_SCHEMA = "public"

# Owner name
_OWNER = "uqa"


class InformationSchemaProvider:
    """Builds information_schema views from UQA Engine metadata."""

    # Registry mapping view name to builder method name
    _VIEWS: dict[str, str] = {
        "schemata": "_build_schemata",
        "tables": "_build_tables",
        "columns": "_build_columns",
        "table_constraints": "_build_table_constraints",
        "key_column_usage": "_build_key_column_usage",
        "referential_constraints": "_build_referential_constraints",
        "constraint_column_usage": "_build_constraint_column_usage",
        "check_constraints": "_build_check_constraints",
        "views": "_build_views",
        "sequences": "_build_sequences",
        "routines": "_build_routines",
        "parameters": "_build_parameters",
        "foreign_tables": "_build_foreign_tables",
        "foreign_servers": "_build_foreign_servers",
        "foreign_server_options": "_build_foreign_server_options",
        "foreign_table_options": "_build_foreign_table_options",
        "enabled_roles": "_build_enabled_roles",
        "applicable_roles": "_build_applicable_roles",
        "character_sets": "_build_character_sets",
        "collations": "_build_collations",
        "domains": "_build_domains",
        "element_types": "_build_element_types",
        "triggers": "_build_triggers",
    }

    @classmethod
    def supported_views(cls) -> list[str]:
        """Return all supported information_schema view names."""
        return list(cls._VIEWS.keys())

    @classmethod
    def build(
        cls,
        view_name: str,
        engine: Engine,
        oids: OIDAllocator,
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Build a named information_schema view.

        Returns (column_names, rows) or raises ValueError for unknown views.
        """
        method_name = cls._VIEWS.get(view_name)
        if method_name is None:
            raise ValueError(f"Unknown information_schema view: '{view_name}'")
        method = getattr(cls, method_name)
        return method(engine, oids)

    # ==================================================================
    # information_schema.schemata
    # ==================================================================

    @staticmethod
    def _build_schemata(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "catalog_name",
            "schema_name",
            "schema_owner",
            "default_character_set_catalog",
            "default_character_set_schema",
            "default_character_set_name",
            "sql_path",
        ]
        rows: list[dict[str, Any]] = [
            {
                "catalog_name": _CATALOG,
                "schema_name": "public",
                "schema_owner": _OWNER,
                "default_character_set_catalog": None,
                "default_character_set_schema": None,
                "default_character_set_name": None,
                "sql_path": None,
            },
            {
                "catalog_name": _CATALOG,
                "schema_name": "information_schema",
                "schema_owner": _OWNER,
                "default_character_set_catalog": None,
                "default_character_set_schema": None,
                "default_character_set_name": None,
                "sql_path": None,
            },
            {
                "catalog_name": _CATALOG,
                "schema_name": "pg_catalog",
                "schema_owner": _OWNER,
                "default_character_set_catalog": None,
                "default_character_set_schema": None,
                "default_character_set_name": None,
                "sql_path": None,
            },
        ]
        return columns, rows

    # ==================================================================
    # information_schema.tables
    # ==================================================================

    @staticmethod
    def _build_tables(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "table_catalog",
            "table_schema",
            "table_name",
            "table_type",
            "self_referencing_column_name",
            "reference_generation",
            "user_defined_type_catalog",
            "user_defined_type_schema",
            "user_defined_type_name",
            "is_insertable_into",
            "is_typed",
            "commit_action",
        ]
        rows: list[dict[str, Any]] = []

        for tname in sorted(engine._tables):
            is_temp = tname in getattr(engine, "_temp_tables", set())
            rows.append(
                {
                    "table_catalog": _CATALOG,
                    "table_schema": _SCHEMA,
                    "table_name": tname,
                    "table_type": "LOCAL TEMPORARY" if is_temp else "BASE TABLE",
                    "self_referencing_column_name": None,
                    "reference_generation": None,
                    "user_defined_type_catalog": None,
                    "user_defined_type_schema": None,
                    "user_defined_type_name": None,
                    "is_insertable_into": "YES",
                    "is_typed": "NO",
                    "commit_action": None,
                }
            )

        for vname in sorted(engine._views):
            rows.append(
                {
                    "table_catalog": _CATALOG,
                    "table_schema": _SCHEMA,
                    "table_name": vname,
                    "table_type": "VIEW",
                    "self_referencing_column_name": None,
                    "reference_generation": None,
                    "user_defined_type_catalog": None,
                    "user_defined_type_schema": None,
                    "user_defined_type_name": None,
                    "is_insertable_into": "NO",
                    "is_typed": "NO",
                    "commit_action": None,
                }
            )

        for ftname in sorted(engine._foreign_tables):
            rows.append(
                {
                    "table_catalog": _CATALOG,
                    "table_schema": _SCHEMA,
                    "table_name": ftname,
                    "table_type": "FOREIGN",
                    "self_referencing_column_name": None,
                    "reference_generation": None,
                    "user_defined_type_catalog": None,
                    "user_defined_type_schema": None,
                    "user_defined_type_name": None,
                    "is_insertable_into": "NO",
                    "is_typed": "NO",
                    "commit_action": None,
                }
            )

        return columns, rows

    # ==================================================================
    # information_schema.columns
    # ==================================================================

    @staticmethod
    def _build_columns(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "column_default",
            "is_nullable",
            "data_type",
            "character_maximum_length",
            "character_octet_length",
            "numeric_precision",
            "numeric_precision_radix",
            "numeric_scale",
            "datetime_precision",
            "interval_type",
            "interval_precision",
            "character_set_catalog",
            "character_set_schema",
            "character_set_name",
            "collation_catalog",
            "collation_schema",
            "collation_name",
            "domain_catalog",
            "domain_schema",
            "domain_name",
            "udt_catalog",
            "udt_schema",
            "udt_name",
            "scope_catalog",
            "scope_schema",
            "scope_name",
            "maximum_cardinality",
            "dtd_identifier",
            "is_self_referencing",
            "is_identity",
            "identity_generation",
            "identity_start",
            "identity_increment",
            "identity_maximum",
            "identity_minimum",
            "identity_cycle",
            "is_generated",
            "generation_expression",
            "is_updatable",
        ]
        rows: list[dict[str, Any]] = []

        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            for pos, (cname, cdef) in enumerate(table.columns.items(), 1):
                display = canonical_type_name(cdef.type_name)
                udt_name = _udt_name(cdef.type_name)

                default_str = None
                if cdef.auto_increment:
                    default_str = f"nextval('{tname}_{cname}_seq'::regclass)"
                elif cdef.default is not None:
                    default_str = _format_default(cdef.default)

                is_identity = "YES" if cdef.auto_increment else "NO"
                identity_gen = "BY DEFAULT" if cdef.auto_increment else None

                dt_precision = None
                if cdef.type_name in (
                    "timestamp",
                    "timestamptz",
                    "timestamp without time zone",
                    "timestamp with time zone",
                    "date",
                    "time",
                ):
                    dt_precision = 6  # microsecond precision

                rows.append(
                    {
                        "table_catalog": _CATALOG,
                        "table_schema": _SCHEMA,
                        "table_name": tname,
                        "column_name": cname,
                        "ordinal_position": pos,
                        "column_default": default_str,
                        "is_nullable": "NO" if cdef.not_null else "YES",
                        "data_type": display,
                        "character_maximum_length": character_maximum_length(
                            cdef.type_name
                        ),
                        "character_octet_length": character_octet_length(
                            cdef.type_name
                        ),
                        "numeric_precision": (
                            cdef.numeric_precision
                            if cdef.numeric_precision is not None
                            else numeric_precision(cdef.type_name)
                        ),
                        "numeric_precision_radix": numeric_precision_radix(
                            cdef.type_name
                        ),
                        "numeric_scale": (
                            cdef.numeric_scale
                            if cdef.numeric_scale is not None
                            else numeric_scale(cdef.type_name)
                        ),
                        "datetime_precision": dt_precision,
                        "interval_type": None,
                        "interval_precision": None,
                        "character_set_catalog": None,
                        "character_set_schema": None,
                        "character_set_name": None,
                        "collation_catalog": None,
                        "collation_schema": None,
                        "collation_name": None,
                        "domain_catalog": None,
                        "domain_schema": None,
                        "domain_name": None,
                        "udt_catalog": _CATALOG,
                        "udt_schema": "pg_catalog",
                        "udt_name": udt_name,
                        "scope_catalog": None,
                        "scope_schema": None,
                        "scope_name": None,
                        "maximum_cardinality": None,
                        "dtd_identifier": str(pos),
                        "is_self_referencing": "NO",
                        "is_identity": is_identity,
                        "identity_generation": identity_gen,
                        "identity_start": "1" if cdef.auto_increment else None,
                        "identity_increment": "1" if cdef.auto_increment else None,
                        "identity_maximum": None,
                        "identity_minimum": None,
                        "identity_cycle": "NO" if cdef.auto_increment else None,
                        "is_generated": "NEVER",
                        "generation_expression": None,
                        "is_updatable": "YES",
                    }
                )

        # Foreign table columns
        for ftname in sorted(engine._foreign_tables):
            ft = engine._foreign_tables[ftname]
            for pos, (cname, cdef) in enumerate(ft.columns.items(), 1):
                display = canonical_type_name(cdef.type_name)
                udt_name = _udt_name(cdef.type_name)
                rows.append(
                    {
                        "table_catalog": _CATALOG,
                        "table_schema": _SCHEMA,
                        "table_name": ftname,
                        "column_name": cname,
                        "ordinal_position": pos,
                        "column_default": None,
                        "is_nullable": "YES",
                        "data_type": display,
                        "character_maximum_length": None,
                        "character_octet_length": None,
                        "numeric_precision": numeric_precision(cdef.type_name),
                        "numeric_precision_radix": numeric_precision_radix(
                            cdef.type_name
                        ),
                        "numeric_scale": numeric_scale(cdef.type_name),
                        "datetime_precision": None,
                        "interval_type": None,
                        "interval_precision": None,
                        "character_set_catalog": None,
                        "character_set_schema": None,
                        "character_set_name": None,
                        "collation_catalog": None,
                        "collation_schema": None,
                        "collation_name": None,
                        "domain_catalog": None,
                        "domain_schema": None,
                        "domain_name": None,
                        "udt_catalog": _CATALOG,
                        "udt_schema": "pg_catalog",
                        "udt_name": udt_name,
                        "scope_catalog": None,
                        "scope_schema": None,
                        "scope_name": None,
                        "maximum_cardinality": None,
                        "dtd_identifier": str(pos),
                        "is_self_referencing": "NO",
                        "is_identity": "NO",
                        "identity_generation": None,
                        "identity_start": None,
                        "identity_increment": None,
                        "identity_maximum": None,
                        "identity_minimum": None,
                        "identity_cycle": None,
                        "is_generated": "NEVER",
                        "generation_expression": None,
                        "is_updatable": "NO",
                    }
                )

        return columns, rows

    # ==================================================================
    # information_schema.table_constraints
    # ==================================================================

    @staticmethod
    def _build_table_constraints(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "constraint_type",
            "is_deferrable",
            "initially_deferred",
            "enforced",
            "nulls_distinct",
        ]
        rows: list[dict[str, Any]] = []

        for tname in sorted(engine._tables):
            table = engine._tables[tname]

            # PRIMARY KEY
            if table.primary_key:
                rows.append(
                    {
                        "constraint_catalog": _CATALOG,
                        "constraint_schema": _SCHEMA,
                        "constraint_name": f"{tname}_pkey",
                        "table_catalog": _CATALOG,
                        "table_schema": _SCHEMA,
                        "table_name": tname,
                        "constraint_type": "PRIMARY KEY",
                        "is_deferrable": "NO",
                        "initially_deferred": "NO",
                        "enforced": "YES",
                        "nulls_distinct": None,
                    }
                )

            # UNIQUE constraints
            for cname, cdef in table.columns.items():
                if cdef.unique and not cdef.primary_key:
                    rows.append(
                        {
                            "constraint_catalog": _CATALOG,
                            "constraint_schema": _SCHEMA,
                            "constraint_name": f"{tname}_{cname}_key",
                            "table_catalog": _CATALOG,
                            "table_schema": _SCHEMA,
                            "table_name": tname,
                            "constraint_type": "UNIQUE",
                            "is_deferrable": "NO",
                            "initially_deferred": "NO",
                            "enforced": "YES",
                            "nulls_distinct": "YES",
                        }
                    )

            # FOREIGN KEY constraints
            for fk in table.foreign_keys:
                rows.append(
                    {
                        "constraint_catalog": _CATALOG,
                        "constraint_schema": _SCHEMA,
                        "constraint_name": f"{tname}_{fk.column}_fkey",
                        "table_catalog": _CATALOG,
                        "table_schema": _SCHEMA,
                        "table_name": tname,
                        "constraint_type": "FOREIGN KEY",
                        "is_deferrable": "NO",
                        "initially_deferred": "NO",
                        "enforced": "YES",
                        "nulls_distinct": None,
                    }
                )

            # CHECK constraints
            for check_name, _ in table.check_constraints:
                rows.append(
                    {
                        "constraint_catalog": _CATALOG,
                        "constraint_schema": _SCHEMA,
                        "constraint_name": f"{tname}_{check_name}_check",
                        "table_catalog": _CATALOG,
                        "table_schema": _SCHEMA,
                        "table_name": tname,
                        "constraint_type": "CHECK",
                        "is_deferrable": "NO",
                        "initially_deferred": "NO",
                        "enforced": "YES",
                        "nulls_distinct": None,
                    }
                )

        return columns, rows

    # ==================================================================
    # information_schema.key_column_usage
    # ==================================================================

    @staticmethod
    def _build_key_column_usage(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "position_in_unique_constraint",
        ]
        rows: list[dict[str, Any]] = []

        for tname in sorted(engine._tables):
            table = engine._tables[tname]

            # PK columns
            if table.primary_key:
                rows.append(
                    {
                        "constraint_catalog": _CATALOG,
                        "constraint_schema": _SCHEMA,
                        "constraint_name": f"{tname}_pkey",
                        "table_catalog": _CATALOG,
                        "table_schema": _SCHEMA,
                        "table_name": tname,
                        "column_name": table.primary_key,
                        "ordinal_position": 1,
                        "position_in_unique_constraint": None,
                    }
                )

            # UNIQUE columns
            for cname, cdef in table.columns.items():
                if cdef.unique and not cdef.primary_key:
                    rows.append(
                        {
                            "constraint_catalog": _CATALOG,
                            "constraint_schema": _SCHEMA,
                            "constraint_name": f"{tname}_{cname}_key",
                            "table_catalog": _CATALOG,
                            "table_schema": _SCHEMA,
                            "table_name": tname,
                            "column_name": cname,
                            "ordinal_position": 1,
                            "position_in_unique_constraint": None,
                        }
                    )

            # FK columns
            for fk in table.foreign_keys:
                # position_in_unique_constraint: ordinal position of
                # the referenced column in its UNIQUE/PK constraint.
                ref_table = engine._tables.get(fk.ref_table)
                ref_pos = None
                if ref_table is not None:
                    ref_cols = list(ref_table.columns.keys())
                    if fk.ref_column in ref_cols:
                        ref_pos = 1  # single-column PK/UNIQUE

                rows.append(
                    {
                        "constraint_catalog": _CATALOG,
                        "constraint_schema": _SCHEMA,
                        "constraint_name": f"{tname}_{fk.column}_fkey",
                        "table_catalog": _CATALOG,
                        "table_schema": _SCHEMA,
                        "table_name": tname,
                        "column_name": fk.column,
                        "ordinal_position": 1,
                        "position_in_unique_constraint": ref_pos,
                    }
                )

        return columns, rows

    # ==================================================================
    # information_schema.referential_constraints
    # ==================================================================

    @staticmethod
    def _build_referential_constraints(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "unique_constraint_catalog",
            "unique_constraint_schema",
            "unique_constraint_name",
            "match_option",
            "update_rule",
            "delete_rule",
        ]
        rows: list[dict[str, Any]] = []

        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            for fk in table.foreign_keys:
                # Find referenced constraint name
                ref_table = engine._tables.get(fk.ref_table)
                ref_constraint = None
                if ref_table is not None:
                    if ref_table.primary_key == fk.ref_column:
                        ref_constraint = f"{fk.ref_table}_pkey"
                    else:
                        ref_constraint = f"{fk.ref_table}_{fk.ref_column}_key"

                rows.append(
                    {
                        "constraint_catalog": _CATALOG,
                        "constraint_schema": _SCHEMA,
                        "constraint_name": f"{tname}_{fk.column}_fkey",
                        "unique_constraint_catalog": _CATALOG,
                        "unique_constraint_schema": _SCHEMA,
                        "unique_constraint_name": ref_constraint,
                        "match_option": "NONE",
                        "update_rule": "NO ACTION",
                        "delete_rule": "NO ACTION",
                    }
                )

        return columns, rows

    # ==================================================================
    # information_schema.constraint_column_usage
    # ==================================================================

    @staticmethod
    def _build_constraint_column_usage(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
        ]
        rows: list[dict[str, Any]] = []

        for tname in sorted(engine._tables):
            table = engine._tables[tname]

            if table.primary_key:
                rows.append(
                    {
                        "table_catalog": _CATALOG,
                        "table_schema": _SCHEMA,
                        "table_name": tname,
                        "column_name": table.primary_key,
                        "constraint_catalog": _CATALOG,
                        "constraint_schema": _SCHEMA,
                        "constraint_name": f"{tname}_pkey",
                    }
                )

            for cname, cdef in table.columns.items():
                if cdef.unique and not cdef.primary_key:
                    rows.append(
                        {
                            "table_catalog": _CATALOG,
                            "table_schema": _SCHEMA,
                            "table_name": tname,
                            "column_name": cname,
                            "constraint_catalog": _CATALOG,
                            "constraint_schema": _SCHEMA,
                            "constraint_name": f"{tname}_{cname}_key",
                        }
                    )

            # FK: the referenced columns
            for fk in table.foreign_keys:
                rows.append(
                    {
                        "table_catalog": _CATALOG,
                        "table_schema": _SCHEMA,
                        "table_name": fk.ref_table,
                        "column_name": fk.ref_column,
                        "constraint_catalog": _CATALOG,
                        "constraint_schema": _SCHEMA,
                        "constraint_name": f"{tname}_{fk.column}_fkey",
                    }
                )

        return columns, rows

    # ==================================================================
    # information_schema.check_constraints
    # ==================================================================

    @staticmethod
    def _build_check_constraints(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "check_clause",
        ]
        rows: list[dict[str, Any]] = []

        for tname in sorted(engine._tables):
            table = engine._tables[tname]

            # NOT NULL constraints (PostgreSQL exposes these as CHECK)
            for cname, cdef in table.columns.items():
                if cdef.not_null and not cdef.primary_key:
                    rows.append(
                        {
                            "constraint_catalog": _CATALOG,
                            "constraint_schema": _SCHEMA,
                            "constraint_name": f"{tname}_{cname}_not_null",
                            "check_clause": f"{cname} IS NOT NULL",
                        }
                    )

            # Explicit CHECK constraints
            for check_name, _ in table.check_constraints:
                rows.append(
                    {
                        "constraint_catalog": _CATALOG,
                        "constraint_schema": _SCHEMA,
                        "constraint_name": f"{tname}_{check_name}_check",
                        "check_clause": check_name,
                    }
                )

        return columns, rows

    # ==================================================================
    # information_schema.views
    # ==================================================================

    @staticmethod
    def _build_views(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "table_catalog",
            "table_schema",
            "table_name",
            "view_definition",
            "check_option",
            "is_updatable",
            "is_insertable_into",
            "is_trigger_updatable",
            "is_trigger_deletable",
            "is_trigger_insertable_into",
        ]
        rows: list[dict[str, Any]] = []

        for vname in sorted(engine._views):
            rows.append(
                {
                    "table_catalog": _CATALOG,
                    "table_schema": _SCHEMA,
                    "table_name": vname,
                    "view_definition": "",
                    "check_option": "NONE",
                    "is_updatable": "NO",
                    "is_insertable_into": "NO",
                    "is_trigger_updatable": "NO",
                    "is_trigger_deletable": "NO",
                    "is_trigger_insertable_into": "NO",
                }
            )

        return columns, rows

    # ==================================================================
    # information_schema.sequences
    # ==================================================================

    @staticmethod
    def _build_sequences(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "sequence_catalog",
            "sequence_schema",
            "sequence_name",
            "data_type",
            "numeric_precision",
            "numeric_precision_radix",
            "numeric_scale",
            "start_value",
            "minimum_value",
            "maximum_value",
            "increment",
            "cycle_option",
        ]
        rows: list[dict[str, Any]] = []

        for sname in sorted(engine._sequences):
            seq = engine._sequences[sname]
            rows.append(
                {
                    "sequence_catalog": _CATALOG,
                    "sequence_schema": _SCHEMA,
                    "sequence_name": sname,
                    "data_type": "bigint",
                    "numeric_precision": 64,
                    "numeric_precision_radix": 2,
                    "numeric_scale": 0,
                    "start_value": str(seq.get("start", 1)),
                    "minimum_value": "1",
                    "maximum_value": "9223372036854775807",
                    "increment": str(seq.get("increment", 1)),
                    "cycle_option": "NO",
                }
            )

        return columns, rows

    # ==================================================================
    # information_schema.routines
    # ==================================================================

    @staticmethod
    def _build_routines(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "specific_catalog",
            "specific_schema",
            "specific_name",
            "routine_catalog",
            "routine_schema",
            "routine_name",
            "routine_type",
            "data_type",
            "type_udt_catalog",
            "type_udt_schema",
            "type_udt_name",
            "routine_definition",
            "external_language",
            "is_deterministic",
            "security_type",
        ]
        # UQA does not have user-defined functions
        return columns, []

    # ==================================================================
    # information_schema.parameters
    # ==================================================================

    @staticmethod
    def _build_parameters(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "specific_catalog",
            "specific_schema",
            "specific_name",
            "ordinal_position",
            "parameter_mode",
            "is_result",
            "as_locator",
            "parameter_name",
            "data_type",
            "parameter_default",
        ]
        return columns, []

    # ==================================================================
    # information_schema.foreign_tables
    # ==================================================================

    @staticmethod
    def _build_foreign_tables(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "foreign_table_catalog",
            "foreign_table_schema",
            "foreign_table_name",
            "foreign_server_catalog",
            "foreign_server_name",
        ]
        rows: list[dict[str, Any]] = []

        for ftname in sorted(engine._foreign_tables):
            ft = engine._foreign_tables[ftname]
            rows.append(
                {
                    "foreign_table_catalog": _CATALOG,
                    "foreign_table_schema": _SCHEMA,
                    "foreign_table_name": ftname,
                    "foreign_server_catalog": _CATALOG,
                    "foreign_server_name": ft.server_name,
                }
            )

        return columns, rows

    # ==================================================================
    # information_schema.foreign_servers
    # ==================================================================

    @staticmethod
    def _build_foreign_servers(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "foreign_server_catalog",
            "foreign_server_name",
            "foreign_data_wrapper_catalog",
            "foreign_data_wrapper_name",
            "foreign_server_type",
            "foreign_server_version",
            "authorization_identifier",
        ]
        rows: list[dict[str, Any]] = []

        for sname in sorted(engine._foreign_servers):
            srv = engine._foreign_servers[sname]
            rows.append(
                {
                    "foreign_server_catalog": _CATALOG,
                    "foreign_server_name": sname,
                    "foreign_data_wrapper_catalog": _CATALOG,
                    "foreign_data_wrapper_name": srv.fdw_type,
                    "foreign_server_type": None,
                    "foreign_server_version": None,
                    "authorization_identifier": _OWNER,
                }
            )

        return columns, rows

    # ==================================================================
    # information_schema.foreign_server_options
    # ==================================================================

    @staticmethod
    def _build_foreign_server_options(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "foreign_server_catalog",
            "foreign_server_name",
            "option_name",
            "option_value",
        ]
        rows: list[dict[str, Any]] = []

        for sname in sorted(engine._foreign_servers):
            srv = engine._foreign_servers[sname]
            for opt_name, opt_value in sorted(srv.options.items()):
                rows.append(
                    {
                        "foreign_server_catalog": _CATALOG,
                        "foreign_server_name": sname,
                        "option_name": opt_name,
                        "option_value": opt_value,
                    }
                )

        return columns, rows

    # ==================================================================
    # information_schema.foreign_table_options
    # ==================================================================

    @staticmethod
    def _build_foreign_table_options(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "foreign_table_catalog",
            "foreign_table_schema",
            "foreign_table_name",
            "option_name",
            "option_value",
        ]
        rows: list[dict[str, Any]] = []

        for ftname in sorted(engine._foreign_tables):
            ft = engine._foreign_tables[ftname]
            for opt_name, opt_value in sorted(ft.options.items()):
                rows.append(
                    {
                        "foreign_table_catalog": _CATALOG,
                        "foreign_table_schema": _SCHEMA,
                        "foreign_table_name": ftname,
                        "option_name": opt_name,
                        "option_value": opt_value,
                    }
                )

        return columns, rows

    # ==================================================================
    # information_schema.enabled_roles
    # ==================================================================

    @staticmethod
    def _build_enabled_roles(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = ["role_name"]
        return columns, [{"role_name": _OWNER}]

    # ==================================================================
    # information_schema.applicable_roles
    # ==================================================================

    @staticmethod
    def _build_applicable_roles(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = ["grantee", "role_name", "is_grantable"]
        return columns, [
            {
                "grantee": _OWNER,
                "role_name": _OWNER,
                "is_grantable": "YES",
            }
        ]

    # ==================================================================
    # information_schema.character_sets
    # ==================================================================

    @staticmethod
    def _build_character_sets(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "character_set_catalog",
            "character_set_schema",
            "character_set_name",
            "character_repertoire",
            "form_of_use",
            "default_collate_catalog",
            "default_collate_schema",
            "default_collate_name",
        ]
        return columns, [
            {
                "character_set_catalog": None,
                "character_set_schema": None,
                "character_set_name": "UTF8",
                "character_repertoire": "UCS",
                "form_of_use": "UTF8",
                "default_collate_catalog": _CATALOG,
                "default_collate_schema": "pg_catalog",
                "default_collate_name": "en_US.utf8",
            }
        ]

    # ==================================================================
    # information_schema.collations
    # ==================================================================

    @staticmethod
    def _build_collations(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "collation_catalog",
            "collation_schema",
            "collation_name",
            "pad_attribute",
        ]
        return columns, [
            {
                "collation_catalog": _CATALOG,
                "collation_schema": "pg_catalog",
                "collation_name": "en_US.utf8",
                "pad_attribute": "NO PAD",
            }
        ]

    # ==================================================================
    # information_schema.domains
    # ==================================================================

    @staticmethod
    def _build_domains(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "domain_catalog",
            "domain_schema",
            "domain_name",
            "data_type",
            "character_maximum_length",
            "numeric_precision",
            "domain_default",
        ]
        # UQA does not support CREATE DOMAIN
        return columns, []

    # ==================================================================
    # information_schema.element_types
    # ==================================================================

    @staticmethod
    def _build_element_types(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "object_catalog",
            "object_schema",
            "object_name",
            "object_type",
            "collection_type_identifier",
            "data_type",
            "character_maximum_length",
            "numeric_precision",
            "numeric_precision_radix",
            "numeric_scale",
            "dtd_identifier",
        ]
        rows: list[dict[str, Any]] = []

        # Emit element types for array columns
        for tname in sorted(engine._tables):
            table = engine._tables[tname]
            for pos, (_cname, cdef) in enumerate(table.columns.items(), 1):
                if cdef.type_name.endswith("[]"):
                    base = cdef.type_name[:-2]
                    display = canonical_type_name(base)
                    rows.append(
                        {
                            "object_catalog": _CATALOG,
                            "object_schema": _SCHEMA,
                            "object_name": tname,
                            "object_type": "TABLE",
                            "collection_type_identifier": str(pos),
                            "data_type": display,
                            "character_maximum_length": None,
                            "numeric_precision": numeric_precision(base),
                            "numeric_precision_radix": numeric_precision_radix(base),
                            "numeric_scale": numeric_scale(base),
                            "dtd_identifier": str(pos),
                        }
                    )

        return columns, rows

    # ==================================================================
    # information_schema.triggers
    # ==================================================================

    @staticmethod
    def _build_triggers(
        engine: Engine, oids: OIDAllocator
    ) -> tuple[list[str], list[dict[str, Any]]]:
        columns = [
            "trigger_catalog",
            "trigger_schema",
            "trigger_name",
            "event_manipulation",
            "event_object_catalog",
            "event_object_schema",
            "event_object_table",
            "action_order",
            "action_condition",
            "action_statement",
            "action_orientation",
            "action_timing",
        ]
        # UQA does not support triggers
        return columns, []


# ======================================================================
# Helpers
# ======================================================================


def _udt_name(type_name: str) -> str:
    """Map a UQA type name to PostgreSQL internal type name (udt_name)."""
    mapping: dict[str, str] = {
        "integer": "int4",
        "int": "int4",
        "int4": "int4",
        "bigint": "int8",
        "int8": "int8",
        "smallint": "int2",
        "int2": "int2",
        "serial": "int4",
        "bigserial": "int8",
        "text": "text",
        "varchar": "varchar",
        "character varying": "varchar",
        "character": "bpchar",
        "char": "bpchar",
        "name": "name",
        "boolean": "bool",
        "bool": "bool",
        "real": "float4",
        "float": "float4",
        "float4": "float4",
        "double precision": "float8",
        "float8": "float8",
        "numeric": "numeric",
        "decimal": "numeric",
        "date": "date",
        "timestamp": "timestamp",
        "timestamp without time zone": "timestamp",
        "timestamptz": "timestamptz",
        "timestamp with time zone": "timestamptz",
        "json": "json",
        "jsonb": "jsonb",
        "uuid": "uuid",
        "bytea": "bytea",
        "point": "point",
        "vector": "vector",
    }
    if type_name.endswith("[]"):
        base = type_name[:-2]
        base_udt = mapping.get(base, base)
        return f"_{base_udt}"
    return mapping.get(type_name, type_name)


def _format_default(value: Any) -> str:
    """Format a default value as a SQL literal string."""
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'::text"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
