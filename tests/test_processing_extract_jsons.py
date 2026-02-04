import unittest


try:
    from KISTI_DB_Manager.processing import extract_data_from_jsons
except ModuleNotFoundError:
    extract_data_from_jsons = None


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


if __name__ == "__main__":
    unittest.main()
