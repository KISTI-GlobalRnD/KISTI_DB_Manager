import unittest


try:
    from KISTI_DB_Manager.processing import extract_data_from_jsons, extract_rows_from_jsons
except ModuleNotFoundError:
    extract_data_from_jsons = None
    extract_rows_from_jsons = None


class TestExtractDataFromJsons(unittest.TestCase):
    def test_extract_rows_and_excepted(self):
        if extract_data_from_jsons is None:
            self.skipTest("Optional dependency missing (numpy/pandas)")

        jsons = [
            {"id": 1, "a": 1, "items": [{"x": 10}, {"x": 11}]},
            {"id": 2, "a": 2, "items": [{"x": 20}]},
        ]

        df_main, df_subs, excepted = extract_data_from_jsons(jsons, index_key="id", except_keys=["a"], sep="__")

        self.assertEqual(list(df_main.columns), ["id"])
        self.assertIn("items", df_subs)
        self.assertEqual(list(df_subs["items"].columns), ["id", "items__x"])
        self.assertEqual(len(df_subs["items"]), 3)

        self.assertIn("a", excepted)
        self.assertEqual(len(excepted["a"]), 2)
        self.assertEqual(excepted["a"][0]["id"], 1)
        self.assertEqual(excepted["a"][0]["value"], 1)
        self.assertEqual(excepted["a"][0]["__except_path__"], "a")
        self.assertEqual(excepted["a"][0]["__except_raw_type__"], "int")
        self.assertIn("__except_raw_json__", excepted["a"][0])

    def test_extract_rows_excepted_includes_record_context(self):
        if extract_rows_from_jsons is None:
            self.skipTest("Optional dependency missing (numpy/pandas)")

        jsons = [
            {"id": 1, "a": {"x": 10}},
            {"id": 2, "a": 20},
        ]
        contexts = [
            {"source_path": "/tmp/a.jsonl", "line_no": 1, "record_index": 0},
            {"source_path": "/tmp/a.jsonl", "line_no": 2, "record_index": 1},
        ]

        rows_main, sub_rows, excepted = extract_rows_from_jsons(
            jsons,
            index_key="id",
            except_keys=["a"],
            record_contexts=contexts,
        )

        self.assertEqual(len(rows_main), 2)
        self.assertEqual(sub_rows, {})
        self.assertIn("a", excepted)
        self.assertEqual(len(excepted["a"]), 2)
        self.assertEqual(excepted["a"][0]["id"], 1)
        self.assertEqual(excepted["a"][0]["value"], {"x": 10})
        self.assertNotIn("x", excepted["a"][0])
        self.assertEqual(excepted["a"][0]["__source_path__"], "/tmp/a.jsonl")
        self.assertEqual(excepted["a"][0]["__line_no__"], 1)
        self.assertEqual(excepted["a"][0]["__record_index__"], 0)

    def test_extract_rows_excepted_expand_dict_legacy_option(self):
        if extract_rows_from_jsons is None:
            self.skipTest("Optional dependency missing (numpy/pandas)")

        jsons = [{"id": 1, "a": {"x": 10, "y": 20}}]
        rows_main, sub_rows, excepted = extract_rows_from_jsons(
            jsons,
            index_key="id",
            except_keys=["a"],
            excepted_expand_dict=True,
        )

        self.assertEqual(len(rows_main), 1)
        self.assertEqual(sub_rows, {})
        self.assertIn("a", excepted)
        self.assertEqual(len(excepted["a"]), 1)
        self.assertEqual(excepted["a"][0]["id"], 1)
        self.assertEqual(excepted["a"][0]["value"], {"x": 10, "y": 20})
        self.assertEqual(excepted["a"][0]["x"], 10)
        self.assertEqual(excepted["a"][0]["y"], 20)


if __name__ == "__main__":
    unittest.main()
