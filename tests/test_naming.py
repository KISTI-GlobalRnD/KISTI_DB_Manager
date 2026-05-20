import unittest


from KISTI_DB_Manager.naming import (
    MYSQL_IDENTIFIER_MAX_LEN,
    canonicalize_column_names,
    make_index_name,
    quote_mysql_identifier,
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
        self.assertTrue(new_cols[1].endswith("__dup2"))
        self.assertLessEqual(len(new_cols[0]), MYSQL_IDENTIFIER_MAX_LEN)
        self.assertLessEqual(len(new_cols[1]), MYSQL_IDENTIFIER_MAX_LEN)
        self.assertIn(cols[0], mapping)

    def test_truncate_column_names_preserves_leading_hint_on_collision(self):
        tail = "__".join(["shared", "leaf", "value"])
        cols = [
            "alpha__" + ("middle__" * 12) + tail,
            "beta__" + ("middle__" * 12) + tail,
        ]
        new_cols, _mapping = truncate_column_names(cols, sep="__")

        self.assertEqual(len(new_cols), 2)
        self.assertNotEqual(new_cols[0], new_cols[1])
        self.assertTrue(new_cols[1].endswith("__beta"))
        self.assertLessEqual(max(len(col) for col in new_cols), MYSQL_IDENTIFIER_MAX_LEN)

    def test_truncate_column_names_hashes_long_tail_without_empty_identifier(self):
        cols = [
            "prefix__" + ("X" * 80),
            "other__" + ("X" * 80),
        ]
        new_cols, _mapping = truncate_column_names(cols, sep="__")

        self.assertEqual(len(new_cols), 2)
        self.assertTrue(all(new_cols))
        self.assertNotEqual(new_cols[0], new_cols[1])
        self.assertLessEqual(max(len(col) for col in new_cols), MYSQL_IDENTIFIER_MAX_LEN)

    def test_canonicalize_column_names_makes_dot_collisions_explicit(self):
        cols = canonicalize_column_names(["a.b", "a__b", "a.b"], key_sep="__")

        self.assertEqual(cols, ["a__b__dot", "a__b__raw", "a__b__dot_dup2"])

    def test_canonicalize_column_names_is_stable_for_reordered_dot_collisions(self):
        left = canonicalize_column_names(["a.b", "a__b"], key_sep="__")
        right = canonicalize_column_names(["a__b", "a.b"], key_sep="__")

        self.assertEqual(set(left), {"a__b__dot", "a__b__raw"})
        self.assertEqual(set(right), {"a__b__dot", "a__b__raw"})

    def test_canonicalize_column_names_labels_exact_duplicates(self):
        cols = canonicalize_column_names(["title", "title"], key_sep="__")

        self.assertEqual(cols, ["title", "title__dup2"])

    def test_quote_mysql_identifier_escapes_backticks(self):
        self.assertEqual(quote_mysql_identifier("a`b"), "`a``b`")

    def test_make_index_name_deterministic(self):
        name1 = make_index_name("tbl", "col" * 30, max_len=64)
        name2 = make_index_name("tbl", "col" * 30, max_len=64)
        self.assertEqual(name1, name2)
        self.assertLessEqual(len(name1), MYSQL_IDENTIFIER_MAX_LEN)


if __name__ == "__main__":
    unittest.main()
