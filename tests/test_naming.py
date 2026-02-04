import unittest


from KISTI_DB_Manager.naming import (
    MYSQL_IDENTIFIER_MAX_LEN,
    make_index_name,
    truncate_column_names,
    truncate_table_name,
)


class TestNaming(unittest.TestCase):
    def test_truncate_table_name_max_len(self):
        original = "A" * (MYSQL_IDENTIFIER_MAX_LEN + 10)
        truncated = truncate_table_name(original)
        self.assertLessEqual(len(truncated), MYSQL_IDENTIFIER_MAX_LEN)

    def test_truncate_column_names_deduplicates(self):
        cols = [
            "prefix__" + ("X" * 80),
            "prefix__" + ("X" * 80),  # duplicate
        ]
        new_cols, mapping = truncate_column_names(cols, sep="__")
        self.assertEqual(len(new_cols), 2)
        self.assertNotEqual(new_cols[0], new_cols[1])
        self.assertLessEqual(len(new_cols[0]), MYSQL_IDENTIFIER_MAX_LEN)
        self.assertLessEqual(len(new_cols[1]), MYSQL_IDENTIFIER_MAX_LEN)
        self.assertIn(cols[0], mapping)

    def test_make_index_name_deterministic(self):
        name1 = make_index_name("tbl", "col" * 30, max_len=64)
        name2 = make_index_name("tbl", "col" * 30, max_len=64)
        self.assertEqual(name1, name2)
        self.assertLessEqual(len(name1), MYSQL_IDENTIFIER_MAX_LEN)


if __name__ == "__main__":
    unittest.main()
