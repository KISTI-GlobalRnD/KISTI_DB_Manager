import unittest


from KISTI_DB_Manager.namemap import NameMap, is_compatible, load_namemap


class TestNameMap(unittest.TestCase):
    def test_build_and_roundtrip(self):
        nm = NameMap.build(
            table_name="t" * 80,
            columns=["a" * 80, "b" * 80],
            key_sep="__",
        )
        self.assertTrue(nm.table_sql)
        self.assertLessEqual(len(nm.table_sql), 64)
        self.assertEqual(len(nm.columns_sql), 2)
        self.assertEqual(set(nm.column_map.keys()), set(nm.columns_original))

        nm2 = NameMap.from_dict(nm.to_dict())
        self.assertEqual(nm2.table_original, nm.table_original)
        self.assertEqual(nm2.table_sql, nm.table_sql)
        self.assertEqual(nm2.columns_original, nm.columns_original)
        self.assertEqual(nm2.columns_sql, nm.columns_sql)
        self.assertEqual(nm2.column_aliases, nm.column_aliases)

    def test_load_and_compatible(self):
        nm = NameMap.build(table_name="tbl", columns=["a", "b"], key_sep="__")
        loaded = load_namemap(nm.to_dict())
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertTrue(is_compatible(loaded, table_name="tbl", key_sep="__", columns=loaded.columns_original))
        self.assertFalse(is_compatible(loaded, table_name="tbl2", key_sep="__", columns=loaded.columns_original))

    def test_extend_preserves_existing_mappings(self):
        nm = NameMap.build(table_name="tbl", columns=["a", "b"], key_sep="__")
        nm2 = nm.with_additional_columns(["b", "c", "a", "d"])

        self.assertEqual(nm2.column_map["a"], nm.column_map["a"])
        self.assertEqual(nm2.column_map["b"], nm.column_map["b"])
        self.assertIn("c", nm2.column_map)
        self.assertIn("d", nm2.column_map)
        self.assertEqual(nm2.columns_original[:2], ("a", "b"))
        self.assertLessEqual(max(len(x) for x in nm2.columns_sql), 64)
        self.assertEqual(len(set(nm2.columns_sql)), len(nm2.columns_sql))

    def test_build_preserves_dot_collision_columns(self):
        nm = NameMap.build(table_name="tbl", columns=["a.b", "a__b"], key_sep="__")

        self.assertEqual(nm.columns_original, ("a__b__dot", "a__b__raw"))
        self.assertEqual(len(set(nm.columns_sql)), 2)
        self.assertEqual(nm.map_column("a.b"), nm.columns_sql[0])
        self.assertEqual(nm.map_column("a__b"), nm.columns_sql[1])
        self.assertEqual(nm.map_column("a__b__raw"), nm.columns_sql[1])

    def test_duplicate_raw_names_do_not_create_ambiguous_aliases(self):
        nm = NameMap.build(table_name="tbl", columns=["title", "title"], key_sep="__")

        self.assertEqual(nm.columns_original, ("title", "title__dup2"))
        self.assertEqual(nm.column_aliases, {})
        self.assertEqual(nm.map_column("title"), nm.column_map["title"])

    def test_extend_makes_duplicate_new_columns_explicit(self):
        nm = NameMap.build(table_name="tbl", columns=["a"], key_sep="__")
        nm2 = nm.with_additional_columns(["b.c", "b__c", "b.c"])

        self.assertIn("b__c__dot", nm2.column_map)
        self.assertIn("b__c__raw", nm2.column_map)
        self.assertIn("b__c__dot_dup2", nm2.column_map)
        self.assertEqual(nm2.map_column("b__c"), nm2.column_map["b__c__raw"])
        self.assertEqual(len(set(nm2.columns_original)), len(nm2.columns_original))
        self.assertEqual(len(set(nm2.columns_sql)), len(nm2.columns_sql))

    def test_extend_preserves_raw_dot_collision_against_existing_canonical(self):
        nm = NameMap.build(table_name="tbl", columns=["a__b"], key_sep="__")
        nm2 = nm.with_additional_columns(["a.b"])

        self.assertEqual(nm2.columns_original, ("a__b", "a__b__dot"))
        self.assertEqual(len(set(nm2.columns_sql)), len(nm2.columns_sql))
        self.assertEqual(nm2.map_column("a.b"), nm2.column_map["a__b__dot"])

    def test_extend_preserves_raw_column_after_dot_only_existing_alias(self):
        nm = NameMap.build(table_name="tbl", columns=["a.b"], key_sep="__")
        nm2 = nm.with_additional_columns(["a__b"])

        self.assertEqual(nm.columns_original, ("a__b",))
        self.assertEqual(nm.column_aliases, {"a.b": "a__b"})
        self.assertEqual(nm2.columns_original, ("a__b", "a__b__raw"))
        self.assertEqual(nm2.map_column("a.b"), nm2.column_map["a__b"])
        self.assertEqual(nm2.map_column("a__b"), nm2.column_map["a__b"])
        self.assertEqual(nm2.canonicalize_input_columns(["a__b"]), ["a__b__raw"])


if __name__ == "__main__":
    unittest.main()
