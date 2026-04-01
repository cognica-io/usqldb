#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Tests for PostgreSQL 17-compatible information_schema and pg_catalog."""

from __future__ import annotations

import pytest

from usqldb import USQLEngine


@pytest.fixture
def engine():
    """Create a fresh USQLEngine with test schema."""
    e = USQLEngine()
    e.sql(
        "CREATE TABLE departments (  id SERIAL PRIMARY KEY,  name TEXT NOT NULL UNIQUE)"
    )
    e.sql(
        "CREATE TABLE employees ("
        "  id SERIAL PRIMARY KEY,"
        "  dept_id INTEGER REFERENCES departments(id),"
        "  name TEXT NOT NULL,"
        "  email TEXT UNIQUE,"
        "  salary NUMERIC(10,2),"
        "  is_active BOOLEAN DEFAULT true"
        ")"
    )
    e.sql("CREATE VIEW dept_summary AS SELECT name FROM departments")
    e.sql("CREATE SEQUENCE invoice_seq START 1000 INCREMENT 5")
    e.sql("INSERT INTO departments (name) VALUES ('Engineering')")
    e.sql("INSERT INTO departments (name) VALUES ('Sales')")
    e.sql(
        "INSERT INTO employees (dept_id, name, email, salary, is_active) "
        "VALUES (1, 'Alice', 'alice@example.com', 150000.50, true)"
    )
    return e


# ======================================================================
# information_schema tests
# ======================================================================


class TestInformationSchemaSchemata:
    def test_lists_all_schemas(self, engine):
        r = engine.sql("SELECT schema_name FROM information_schema.schemata")
        names = {row["schema_name"] for row in r}
        assert names == {"public", "information_schema", "pg_catalog"}

    def test_catalog_name(self, engine):
        r = engine.sql(
            "SELECT catalog_name FROM information_schema.schemata "
            "WHERE schema_name = 'public'"
        )
        assert r.rows[0]["catalog_name"] == "uqa"


class TestInformationSchemaTables:
    def test_base_tables(self, engine):
        r = engine.sql(
            "SELECT table_name, table_type "
            "FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        )
        names = [row["table_name"] for row in r]
        assert "departments" in names
        assert "employees" in names

    def test_views(self, engine):
        r = engine.sql(
            "SELECT table_name, table_type "
            "FROM information_schema.tables "
            "WHERE table_type = 'VIEW'"
        )
        assert len(r) >= 1
        names = {row["table_name"] for row in r}
        assert "dept_summary" in names

    def test_is_insertable(self, engine):
        r = engine.sql(
            "SELECT table_name, is_insertable_into "
            "FROM information_schema.tables "
            "WHERE table_name = 'departments'"
        )
        assert r.rows[0]["is_insertable_into"] == "YES"


class TestInformationSchemaColumns:
    def test_column_count(self, engine):
        r = engine.sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'employees'"
        )
        assert len(r) == 6

    def test_ordinal_position(self, engine):
        r = engine.sql(
            "SELECT column_name, ordinal_position "
            "FROM information_schema.columns "
            "WHERE table_name = 'employees' "
            "ORDER BY ordinal_position"
        )
        cols = [row["column_name"] for row in r]
        assert cols == ["id", "dept_id", "name", "email", "salary", "is_active"]

    def test_data_types(self, engine):
        r = engine.sql(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = 'employees' "
            "ORDER BY ordinal_position"
        )
        types = {row["column_name"]: row["data_type"] for row in r}
        assert types["id"] == "integer"
        assert types["name"] == "text"
        assert types["salary"] == "numeric"
        assert types["is_active"] == "boolean"

    def test_nullable(self, engine):
        r = engine.sql(
            "SELECT column_name, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_name = 'employees'"
        )
        nullable = {row["column_name"]: row["is_nullable"] for row in r}
        assert nullable["id"] == "NO"  # PK is NOT NULL
        assert nullable["name"] == "NO"
        assert nullable["email"] == "YES"

    def test_defaults(self, engine):
        r = engine.sql(
            "SELECT column_name, column_default "
            "FROM information_schema.columns "
            "WHERE table_name = 'employees' AND column_default IS NOT NULL"
        )
        defaults = {row["column_name"]: row["column_default"] for row in r}
        assert "nextval" in defaults["id"]
        assert defaults["is_active"] == "true"

    def test_numeric_precision_scale(self, engine):
        r = engine.sql(
            "SELECT numeric_precision, numeric_scale "
            "FROM information_schema.columns "
            "WHERE table_name = 'employees' AND column_name = 'salary'"
        )
        assert r.rows[0]["numeric_precision"] == 10
        assert r.rows[0]["numeric_scale"] == 2

    def test_udt_name(self, engine):
        r = engine.sql(
            "SELECT column_name, udt_name "
            "FROM information_schema.columns "
            "WHERE table_name = 'employees' "
            "ORDER BY ordinal_position"
        )
        udt = {row["column_name"]: row["udt_name"] for row in r}
        assert udt["id"] == "int4"
        assert udt["name"] == "text"
        assert udt["salary"] == "numeric"
        assert udt["is_active"] == "bool"

    def test_identity(self, engine):
        r = engine.sql(
            "SELECT column_name, is_identity, identity_generation "
            "FROM information_schema.columns "
            "WHERE table_name = 'employees' AND is_identity = 'YES'"
        )
        assert len(r) == 1
        assert r.rows[0]["column_name"] == "id"
        assert r.rows[0]["identity_generation"] == "BY DEFAULT"


class TestInformationSchemaConstraints:
    def test_primary_keys(self, engine):
        r = engine.sql(
            "SELECT constraint_name, table_name "
            "FROM information_schema.table_constraints "
            "WHERE constraint_type = 'PRIMARY KEY' "
            "ORDER BY table_name"
        )
        names = {row["table_name"]: row["constraint_name"] for row in r}
        assert names["departments"] == "departments_pkey"
        assert names["employees"] == "employees_pkey"

    def test_unique_constraints(self, engine):
        r = engine.sql(
            "SELECT constraint_name, table_name "
            "FROM information_schema.table_constraints "
            "WHERE constraint_type = 'UNIQUE'"
        )
        names = {row["constraint_name"] for row in r}
        assert "departments_name_key" in names
        assert "employees_email_key" in names

    def test_foreign_keys(self, engine):
        r = engine.sql(
            "SELECT constraint_name, table_name "
            "FROM information_schema.table_constraints "
            "WHERE constraint_type = 'FOREIGN KEY'"
        )
        assert len(r) == 1
        assert r.rows[0]["constraint_name"] == "employees_dept_id_fkey"
        assert r.rows[0]["table_name"] == "employees"

    def test_key_column_usage(self, engine):
        r = engine.sql(
            "SELECT constraint_name, column_name "
            "FROM information_schema.key_column_usage "
            "WHERE table_name = 'employees'"
        )
        cols = {row["constraint_name"]: row["column_name"] for row in r}
        assert cols["employees_pkey"] == "id"
        assert cols["employees_email_key"] == "email"
        assert cols["employees_dept_id_fkey"] == "dept_id"

    def test_referential_constraints(self, engine):
        r = engine.sql(
            "SELECT constraint_name, unique_constraint_name, "
            "update_rule, delete_rule "
            "FROM information_schema.referential_constraints"
        )
        assert len(r) == 1
        row = r.rows[0]
        assert row["constraint_name"] == "employees_dept_id_fkey"
        assert row["unique_constraint_name"] == "departments_pkey"
        assert row["update_rule"] == "NO ACTION"
        assert row["delete_rule"] == "NO ACTION"

    def test_constraint_column_usage(self, engine):
        r = engine.sql(
            "SELECT table_name, column_name, constraint_name "
            "FROM information_schema.constraint_column_usage "
            "WHERE constraint_name = 'employees_dept_id_fkey'"
        )
        assert len(r) == 1
        assert r.rows[0]["table_name"] == "departments"
        assert r.rows[0]["column_name"] == "id"


class TestInformationSchemaViews:
    def test_view_listing(self, engine):
        r = engine.sql("SELECT table_name, is_updatable FROM information_schema.views")
        assert len(r) >= 1
        names = {row["table_name"] for row in r}
        assert "dept_summary" in names

    def test_check_option(self, engine):
        r = engine.sql(
            "SELECT check_option FROM information_schema.views "
            "WHERE table_name = 'dept_summary'"
        )
        assert r.rows[0]["check_option"] == "NONE"


class TestInformationSchemaSequences:
    def test_sequence_listing(self, engine):
        r = engine.sql(
            "SELECT sequence_name, data_type, start_value, increment "
            "FROM information_schema.sequences"
        )
        assert len(r) == 1
        assert r.rows[0]["sequence_name"] == "invoice_seq"
        assert r.rows[0]["data_type"] == "bigint"
        assert r.rows[0]["start_value"] == "1000"
        assert r.rows[0]["increment"] == "5"


class TestInformationSchemaOther:
    def test_enabled_roles(self, engine):
        r = engine.sql("SELECT role_name FROM information_schema.enabled_roles")
        assert len(r) == 1
        assert r.rows[0]["role_name"] == "uqa"

    def test_character_sets(self, engine):
        r = engine.sql(
            "SELECT character_set_name FROM information_schema.character_sets"
        )
        assert r.rows[0]["character_set_name"] == "UTF8"

    def test_empty_domains(self, engine):
        r = engine.sql("SELECT * FROM information_schema.domains")
        assert len(r) == 0

    def test_empty_triggers(self, engine):
        r = engine.sql("SELECT * FROM information_schema.triggers")
        assert len(r) == 0


# ======================================================================
# pg_catalog tests
# ======================================================================


class TestPGNamespace:
    def test_standard_schemas(self, engine):
        r = engine.sql("SELECT oid, nspname FROM pg_catalog.pg_namespace ORDER BY oid")
        schemas = {row["nspname"]: row["oid"] for row in r}
        assert schemas["pg_catalog"] == 11
        assert schemas["public"] == 2200
        assert schemas["information_schema"] == 13182


class TestPGClass:
    def test_tables_present(self, engine):
        r = engine.sql(
            "SELECT relname, relkind FROM pg_catalog.pg_class "
            "WHERE relnamespace = 2200 AND relkind = 'r'"
        )
        names = {row["relname"] for row in r}
        assert "departments" in names
        assert "employees" in names

    def test_indexes_present(self, engine):
        r = engine.sql(
            "SELECT relname FROM pg_catalog.pg_class "
            "WHERE relnamespace = 2200 AND relkind = 'i'"
        )
        names = {row["relname"] for row in r}
        assert "departments_pkey" in names
        assert "employees_pkey" in names
        assert "departments_name_key" in names
        assert "employees_email_key" in names

    def test_views_present(self, engine):
        r = engine.sql(
            "SELECT relname FROM pg_catalog.pg_class "
            "WHERE relnamespace = 2200 AND relkind = 'v'"
        )
        names = {row["relname"] for row in r}
        assert "dept_summary" in names

    def test_sequences_present(self, engine):
        r = engine.sql(
            "SELECT relname FROM pg_catalog.pg_class "
            "WHERE relnamespace = 2200 AND relkind = 'S'"
        )
        names = {row["relname"] for row in r}
        assert "invoice_seq" in names


class TestPGAttribute:
    def test_column_types(self, engine):
        r = engine.sql(
            "SELECT a.attname, a.atttypid, a.attnum "
            "FROM pg_catalog.pg_attribute a "
            "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
            "WHERE c.relname = 'employees' AND a.attnum > 0 "
            "ORDER BY a.attnum"
        )
        cols = [(row["attname"], row["atttypid"]) for row in r]
        assert cols[0] == ("id", 23)  # int4
        assert cols[1] == ("dept_id", 23)  # int4
        assert cols[2] == ("name", 25)  # text
        assert cols[3] == ("email", 25)  # text
        assert cols[4] == ("salary", 1700)  # numeric
        assert cols[5] == ("is_active", 16)  # bool

    def test_system_columns(self, engine):
        r = engine.sql(
            "SELECT a.attname, a.attnum "
            "FROM pg_catalog.pg_attribute a "
            "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
            "WHERE c.relname = 'departments' AND a.attnum < 0 "
            "ORDER BY a.attnum"
        )
        sys_cols = {row["attname"] for row in r}
        assert "ctid" in sys_cols
        assert "xmin" in sys_cols
        assert "tableoid" in sys_cols


class TestPGType:
    def test_base_types(self, engine):
        r = engine.sql(
            "SELECT oid, typname FROM pg_catalog.pg_type "
            "WHERE oid IN (16, 23, 25, 701, 1700)"
        )
        types = {row["oid"]: row["typname"] for row in r}
        assert types[16] == "bool"
        assert types[23] == "int4"
        assert types[25] == "text"
        assert types[701] == "float8"
        assert types[1700] == "numeric"

    def test_array_types(self, engine):
        r = engine.sql(
            "SELECT typname, typelem FROM pg_catalog.pg_type "
            "WHERE typcategory = 'A' AND typelem = 23"
        )
        assert len(r) >= 1
        assert r.rows[0]["typname"] == "_int4"

    def test_composite_types(self, engine):
        r = engine.sql(
            "SELECT typname, typtype FROM pg_catalog.pg_type WHERE typtype = 'c'"
        )
        names = {row["typname"] for row in r}
        assert "departments" in names
        assert "employees" in names


class TestPGConstraint:
    def test_primary_key(self, engine):
        r = engine.sql(
            "SELECT conname, contype FROM pg_catalog.pg_constraint WHERE contype = 'p'"
        )
        names = {row["conname"] for row in r}
        assert "departments_pkey" in names
        assert "employees_pkey" in names

    def test_foreign_key(self, engine):
        r = engine.sql(
            "SELECT con.conname, src.relname AS src, tgt.relname AS tgt "
            "FROM pg_catalog.pg_constraint con "
            "JOIN pg_catalog.pg_class src ON con.conrelid = src.oid "
            "JOIN pg_catalog.pg_class tgt ON con.confrelid = tgt.oid "
            "WHERE con.contype = 'f'"
        )
        assert len(r) == 1
        assert r.rows[0]["conname"] == "employees_dept_id_fkey"
        assert r.rows[0]["src"] == "employees"
        assert r.rows[0]["tgt"] == "departments"

    def test_unique_constraint(self, engine):
        r = engine.sql(
            "SELECT conname FROM pg_catalog.pg_constraint WHERE contype = 'u'"
        )
        names = {row["conname"] for row in r}
        assert "departments_name_key" in names
        assert "employees_email_key" in names


class TestPGIndex:
    def test_primary_index(self, engine):
        r = engine.sql(
            "SELECT i.indisprimary, c.relname "
            "FROM pg_catalog.pg_index i "
            "JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid "
            "WHERE i.indisprimary = 1"
        )
        names = {row["relname"] for row in r}
        assert "departments_pkey" in names
        assert "employees_pkey" in names

    def test_unique_index(self, engine):
        r = engine.sql(
            "SELECT c.relname "
            "FROM pg_catalog.pg_index i "
            "JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid "
            "WHERE i.indisunique = 1 AND i.indisprimary = 0"
        )
        names = {row["relname"] for row in r}
        assert "departments_name_key" in names
        assert "employees_email_key" in names


class TestPGSettings:
    def test_server_version(self, engine):
        r = engine.sql(
            "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'server_version'"
        )
        assert r.rows[0]["setting"] == "17.0"

    def test_server_version_num(self, engine):
        r = engine.sql(
            "SELECT setting FROM pg_catalog.pg_settings "
            "WHERE name = 'server_version_num'"
        )
        assert r.rows[0]["setting"] == "170000"

    def test_encoding(self, engine):
        r = engine.sql(
            "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'server_encoding'"
        )
        assert r.rows[0]["setting"] == "UTF8"


class TestPGOther:
    def test_pg_database(self, engine):
        r = engine.sql("SELECT datname FROM pg_catalog.pg_database")
        assert r.rows[0]["datname"] == "uqa"

    def test_pg_roles(self, engine):
        r = engine.sql("SELECT rolname, rolsuper FROM pg_catalog.pg_roles")
        assert r.rows[0]["rolname"] == "uqa"
        assert r.rows[0]["rolsuper"] == 1

    def test_pg_am(self, engine):
        r = engine.sql("SELECT amname FROM pg_catalog.pg_am ORDER BY amname")
        names = [row["amname"] for row in r]
        assert "btree" in names
        assert "heap" in names
        assert "hnsw" in names

    def test_pg_tables(self, engine):
        r = engine.sql(
            "SELECT tablename FROM pg_catalog.pg_tables "
            "WHERE schemaname = 'public' ORDER BY tablename"
        )
        names = [row["tablename"] for row in r]
        assert "departments" in names
        assert "employees" in names

    def test_pg_views(self, engine):
        r = engine.sql("SELECT viewname FROM pg_catalog.pg_views")
        names = {row["viewname"] for row in r}
        assert "dept_summary" in names

    def test_pg_indexes(self, engine):
        r = engine.sql(
            "SELECT indexname FROM pg_catalog.pg_indexes WHERE tablename = 'employees'"
        )
        names = {row["indexname"] for row in r}
        assert "employees_pkey" in names
        assert "employees_email_key" in names

    def test_pg_sequences(self, engine):
        r = engine.sql(
            "SELECT sequencename, start_value, increment_by "
            "FROM pg_catalog.pg_sequences"
        )
        assert r.rows[0]["sequencename"] == "invoice_seq"
        assert r.rows[0]["start_value"] == 1000
        assert r.rows[0]["increment_by"] == 5

    def test_pg_stat_user_tables(self, engine):
        r = engine.sql(
            "SELECT relname, n_live_tup "
            "FROM pg_catalog.pg_stat_user_tables ORDER BY relname"
        )
        stats = {row["relname"]: row["n_live_tup"] for row in r}
        assert stats["departments"] == 2
        assert stats["employees"] == 1

    def test_pg_stat_activity(self, engine):
        r = engine.sql("SELECT datname, state FROM pg_catalog.pg_stat_activity")
        assert r.rows[0]["datname"] == "uqa"
        assert r.rows[0]["state"] == "active"

    def test_pg_extension(self, engine):
        r = engine.sql("SELECT extname FROM pg_catalog.pg_extension")
        assert r.rows[0]["extname"] == "plpgsql"

    def test_pg_collation(self, engine):
        r = engine.sql("SELECT collname FROM pg_catalog.pg_collation")
        names = {row["collname"] for row in r}
        assert "default" in names
        assert "C" in names

    def test_pg_description_empty(self, engine):
        r = engine.sql("SELECT * FROM pg_catalog.pg_description")
        assert len(r) == 0

    def test_pg_enum_empty(self, engine):
        r = engine.sql("SELECT * FROM pg_catalog.pg_enum")
        assert len(r) == 0

    def test_pg_inherits_empty(self, engine):
        r = engine.sql("SELECT * FROM pg_catalog.pg_inherits")
        assert len(r) == 0

    def test_pg_trigger_empty(self, engine):
        r = engine.sql("SELECT * FROM pg_catalog.pg_trigger")
        assert len(r) == 0


class TestPGAttrdef:
    def test_serial_defaults(self, engine):
        r = engine.sql(
            "SELECT d.adbin "
            "FROM pg_catalog.pg_attrdef d "
            "JOIN pg_catalog.pg_class c ON d.adrelid = c.oid "
            "WHERE c.relname = 'employees'"
        )
        defaults = [row["adbin"] for row in r]
        assert any("nextval" in d for d in defaults)


# ======================================================================
# OID cross-reference consistency tests
# ======================================================================


class TestOIDConsistency:
    def test_pg_class_pg_attribute_join(self, engine):
        """OIDs in pg_class.oid must match pg_attribute.attrelid."""
        r = engine.sql(
            "SELECT c.relname, a.attname "
            "FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid "
            "WHERE c.relname = 'departments' AND a.attnum > 0 "
            "ORDER BY a.attnum"
        )
        cols = [row["attname"] for row in r]
        assert cols == ["id", "name"]

    def test_pg_attribute_pg_type_join(self, engine):
        """OIDs in pg_attribute.atttypid must match pg_type.oid."""
        r = engine.sql(
            "SELECT a.attname, t.typname "
            "FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid "
            "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid "
            "WHERE c.relname = 'employees' AND a.attnum > 0 "
            "ORDER BY a.attnum"
        )
        type_map = {row["attname"]: row["typname"] for row in r}
        assert type_map["id"] == "int4"
        assert type_map["name"] == "text"
        assert type_map["salary"] == "numeric"
        assert type_map["is_active"] == "bool"

    def test_pg_constraint_pg_class_join(self, engine):
        """OIDs in pg_constraint.conrelid must match pg_class.oid."""
        r = engine.sql(
            "SELECT con.conname, c.relname "
            "FROM pg_catalog.pg_constraint con "
            "JOIN pg_catalog.pg_class c ON con.conrelid = c.oid "
            "WHERE con.contype = 'p'"
        )
        pks = {row["relname"]: row["conname"] for row in r}
        assert pks["departments"] == "departments_pkey"
        assert pks["employees"] == "employees_pkey"

    def test_pg_index_pg_class_join(self, engine):
        """OIDs in pg_index must match pg_class for both index and table."""
        r = engine.sql(
            "SELECT idx.relname AS index_name, tbl.relname AS table_name "
            "FROM pg_catalog.pg_index i "
            "JOIN pg_catalog.pg_class idx ON i.indexrelid = idx.oid "
            "JOIN pg_catalog.pg_class tbl ON i.indrelid = tbl.oid "
            "WHERE i.indisprimary = 1"
        )
        pk_map = {row["table_name"]: row["index_name"] for row in r}
        assert pk_map["departments"] == "departments_pkey"
        assert pk_map["employees"] == "employees_pkey"

    def test_pg_constraint_confrelid_join(self, engine):
        """FK constraint confrelid must match referenced table OID."""
        r = engine.sql(
            "SELECT con.conname, ref.relname AS ref_table "
            "FROM pg_catalog.pg_constraint con "
            "JOIN pg_catalog.pg_class ref ON con.confrelid = ref.oid "
            "WHERE con.contype = 'f'"
        )
        assert r.rows[0]["ref_table"] == "departments"


class TestEmptyEngine:
    """Tests against an engine with no user objects."""

    def test_information_schema_tables_empty(self):
        e = USQLEngine()
        r = e.sql(
            "SELECT * FROM information_schema.tables WHERE table_schema = 'public'"
        )
        assert len(r) == 0

    def test_pg_class_empty(self):
        e = USQLEngine()
        r = e.sql("SELECT * FROM pg_catalog.pg_class WHERE relnamespace = 2200")
        assert len(r) == 0

    def test_pg_type_still_populated(self):
        e = USQLEngine()
        r = e.sql("SELECT COUNT(*) AS cnt FROM pg_catalog.pg_type")
        assert r.rows[0]["cnt"] > 20

    def test_pg_settings_still_populated(self):
        e = USQLEngine()
        r = e.sql("SELECT COUNT(*) AS cnt FROM pg_catalog.pg_settings")
        assert r.rows[0]["cnt"] > 10
