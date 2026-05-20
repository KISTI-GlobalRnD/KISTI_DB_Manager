import unittest
import inspect

from KISTI_DB_Manager import manage
from KISTI_DB_Manager.namemap import NameMap


class TestManageColumnComments(unittest.TestCase):
    def test_column_type_with_comment_uses_description_and_escapes_sql_literal(self):
        nm = NameMap.build(
            table_name="works_authorships",
            columns=["author_openalex_id"],
            key_sep="__",
            max_len=64,
        )
        desc_by_sql = manage._column_descriptions_by_sql(
            nm,
            {"author_openalex_id": "OpenAlex author's ID \\ compacted"},
        )

        sql_type = manage._column_type_with_comment("LONGTEXT", "author_openalex_id", desc_by_sql)

        self.assertEqual(sql_type, "LONGTEXT COMMENT 'OpenAlex author''s ID \\\\ compacted'")

    def test_column_type_with_comment_preserves_existing_comment_when_no_description(self):
        sql_type = manage._column_type_with_comment(
            "LONGTEXT",
            "author_openalex_id",
            {},
            {"author_openalex_id": "Existing comment"},
        )

        self.assertEqual(sql_type, "LONGTEXT COMMENT 'Existing comment'")

    def test_column_type_with_comment_prefers_new_description_over_existing_comment(self):
        sql_type = manage._column_type_with_comment(
            "LONGTEXT",
            "author_openalex_id",
            {"author_openalex_id": "New compaction description"},
            {"author_openalex_id": "Existing comment"},
        )

        self.assertEqual(sql_type, "LONGTEXT COMMENT 'New compaction description'")

    def test_load_helpers_accept_column_descriptions(self):
        for fn in (
            manage.fill_table_from_dataframe,
            manage.fill_table_from_rows,
            manage.fill_table_from_tsv_file,
        ):
            self.assertIn("column_descriptions", inspect.signature(fn).parameters)

    def test_generate_create_table_sql_quotes_backticks_in_identifiers(self):
        try:
            import pandas as pd
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pandas is required: {exc}")

        data_config = {"table_name": "tbl`bad", "KEY": "a`b"}
        df_desc = pd.DataFrame(
            {"Type": ["TEXT NOT NULL", "INT"], "Null_ratio": [0.0, 0.0], "is_key": [True, False]},
            index=["a`b", "normal"],
        )

        sql = manage.generate_create_table_sql(data_config, df_desc=df_desc)

        self.assertIn("CREATE TABLE `tbl``bad`", sql)
        self.assertIn("`a``b` TEXT NOT NULL", sql)
        self.assertNotIn("NOT NULL NOT NULL", sql)
        self.assertIn("`normal` INT", sql)

    def test_fill_table_from_rows_renames_raw_dot_collision_keys_before_insert(self):
        try:
            from sqlalchemy import create_engine, text
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"sqlalchemy is required: {exc}")

        engine = create_engine("sqlite:///:memory:")
        nm = NameMap.build(table_name="tbl", columns=["a.b", "a__b"], key_sep="__")
        with engine.begin() as conn:
            conn.execute(text('CREATE TABLE tbl ("a__b__dot" TEXT, "a__b__raw" TEXT)'))

        manage.fill_table_from_rows(
            [{"a.b": "left", "a__b": "right"}],
            {},
            table_name="tbl",
            name_map=nm,
            columns_original=["a.b", "a__b"],
            engine=engine,
            existing_cols=set(nm.columns_sql),
            load_method="to_sql",
        )

        with engine.connect() as conn:
            row = conn.execute(text('SELECT "a__b__dot", "a__b__raw" FROM tbl')).fetchone()

        self.assertEqual(tuple(row), ("left", "right"))

    def test_fill_table_from_rows_separates_later_raw_column_from_dot_alias(self):
        try:
            from sqlalchemy import create_engine, text
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"sqlalchemy is required: {exc}")

        engine = create_engine("sqlite:///:memory:")
        nm = NameMap.build(table_name="tbl", columns=["a.b"], key_sep="__")
        with engine.begin() as conn:
            conn.execute(text('CREATE TABLE tbl ("a__b" TEXT, "a__b__raw" TEXT)'))

        out_nm = manage.fill_table_from_rows(
            [{"a__b": "raw-value"}],
            {},
            table_name="tbl",
            name_map=nm,
            columns_original=["a__b"],
            engine=engine,
            existing_cols={"a__b", "a__b__raw"},
            load_method="to_sql",
        )

        with engine.connect() as conn:
            row = conn.execute(text('SELECT "a__b", "a__b__raw" FROM tbl')).fetchone()

        self.assertEqual(tuple(row), (None, "raw-value"))
        self.assertEqual(out_nm.columns_original, ("a__b", "a__b__raw"))

    def test_fill_table_from_dataframe_extends_namemap_before_schema_drift_insert(self):
        try:
            import pandas as pd
            from sqlalchemy import create_engine, text
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pandas/sqlalchemy are required: {exc}")

        long_col = "prefix__" + ("x" * 80)
        engine = create_engine("sqlite:///:memory:")
        nm = NameMap.build(table_name="tbl", columns=["id"], key_sep="__")
        with engine.begin() as conn:
            conn.execute(text('CREATE TABLE tbl ("id" TEXT)'))

        out_nm = manage.fill_table_from_dataframe(
            pd.DataFrame({"id": ["1"], long_col: ["value"]}),
            {},
            table_name="tbl",
            name_map=nm,
            engine=engine,
            existing_cols={"id"},
            load_method="to_sql",
        )
        sql_col = out_nm.map_column(long_col)

        self.assertIn(sql_col, out_nm.columns_sql)
        self.assertLessEqual(len(sql_col), 64)
        with engine.connect() as conn:
            row = conn.execute(text(f'SELECT "id", "{sql_col}" FROM tbl')).fetchone()

        self.assertEqual(tuple(row), ("1", "value"))


if __name__ == "__main__":
    unittest.main()
