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


if __name__ == "__main__":
    unittest.main()
