import unittest
import tempfile


try:
    from KISTI_DB_Manager.id_compaction import IdCompactor
    from KISTI_DB_Manager.processing import (
        _safe_flatten_jsons_to_tsv_worker,
        extract_data_from_jsons,
        extract_rows_from_jsons,
    )
except ModuleNotFoundError:
    IdCompactor = None
    _safe_flatten_jsons_to_tsv_worker = None
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
        self.assertEqual(excepted["a"][0]["value"], '{"x":10}')
        self.assertNotIn("x", excepted["a"][0])
        self.assertEqual(excepted["a"][0]["__except_raw_json__"], '{"x":10}')
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

    def test_extract_rows_applies_id_compaction_to_main_sub_and_excepted_parent_id(self):
        if extract_rows_from_jsons is None or IdCompactor is None:
            self.skipTest("Optional dependency missing (numpy/pandas)")

        compactor = IdCompactor.from_config({"id_compaction": {"enabled": True}}, sep="__", index_key="id")
        jsons = [
            {
                "id": "https://openalex.org/W1",
                "authorships": [{"author": {"id": "https://openalex.org/A1"}}],
                "referenced_works": ["https://openalex.org/W2"],
                "abstract_inverted_index": {"word": [0]},
            }
        ]

        rows_main, sub_rows, excepted = extract_rows_from_jsons(
            jsons,
            index_key="id",
            base_table="works",
            except_keys=["abstract_inverted_index"],
            id_compactor=compactor,
        )

        self.assertEqual(rows_main[0]["id"], "W1")
        self.assertEqual(sub_rows["authorships"][0]["id"], "W1")
        self.assertEqual(sub_rows["authorships"][0]["authorships__author_openalex_id"], "A1")
        self.assertEqual(sub_rows["referenced_works"][0]["referenced_work_openalex_id"], "W2")
        self.assertEqual(excepted["abstract_inverted_index"][0]["id"], "W1")
        self.assertEqual(excepted["abstract_inverted_index"][0]["__except_raw_json__"], '{"word":[0]}')

    def test_extract_rows_parallel_applies_id_compaction(self):
        if extract_rows_from_jsons is None or IdCompactor is None:
            self.skipTest("Optional dependency missing (numpy/pandas)")

        compactor = IdCompactor.from_config({"id_compaction": {"enabled": True}}, sep="__", index_key="id")

        rows_main, sub_rows, excepted = extract_rows_from_jsons(
            [
                {"id": "https://openalex.org/W1", "referenced_works": ["https://openalex.org/W2"]},
                {"id": "https://openalex.org/W3", "authorships": [{"author": {"id": "https://openalex.org/A1"}}]},
            ],
            index_key="id",
            base_table="works",
            id_compactor=compactor,
            parallel_workers=2,
        )

        self.assertEqual([r["id"] for r in rows_main], ["W1", "W3"])
        self.assertEqual(sub_rows["referenced_works"][0]["referenced_work_openalex_id"], "W2")
        self.assertEqual(sub_rows["authorships"][0]["authorships__author_openalex_id"], "A1")
        self.assertEqual(excepted, {})
        self.assertEqual(compactor.summary()["counts"]["works.id"], 2)
        self.assertEqual(compactor.summary()["counts"]["works__referenced_works.referenced_work_openalex_id"], 1)

    def test_tsv_worker_applies_id_compaction(self):
        if _safe_flatten_jsons_to_tsv_worker is None:
            self.skipTest("Optional dependency missing (numpy/pandas)")

        with tempfile.TemporaryDirectory() as td:
            res = _safe_flatten_jsons_to_tsv_worker(
                (
                    0,
                    [{"id": "https://openalex.org/W1", "referenced_works": ["https://openalex.org/W2"]}],
                    "id",
                    (),
                    "__",
                    td,
                    None,
                    "works",
                    None,
                    None,
                    False,
                    {"enabled": True, "preset": "openalex", "mode": "semantic_column_strip"},
                )
            )

            self.assertTrue(res["ok"])
            self.assertEqual(res["main"]["columns"], ["id"])
            self.assertEqual(res["id_compaction"]["counts"]["works.id"], 1)
            self.assertIn("referenced_work_openalex_id", res["subs"]["referenced_works"]["columns"])
            with open(res["main"]["path"], encoding="utf-8") as f:
                self.assertEqual(f.read().strip(), "W1")

    def test_extract_rows_propagates_id_compaction_collision(self):
        if extract_rows_from_jsons is None or IdCompactor is None:
            self.skipTest("Optional dependency missing (numpy/pandas)")

        compactor = IdCompactor.from_config({"id_compaction": {"enabled": True}}, sep="__", index_key="id")

        with self.assertRaises(Exception) as cm:
            extract_rows_from_jsons(
                [
                    {
                        "id": "https://openalex.org/W1",
                        "author_id": "https://openalex.org/A1",
                        "author_openalex_id": "A2",
                    }
                ],
                index_key="id",
                base_table="works",
                id_compactor=compactor,
            )

        self.assertIn("collision", str(cm.exception))

    def test_extract_rows_parallel_propagates_id_compaction_collision(self):
        if extract_rows_from_jsons is None or IdCompactor is None:
            self.skipTest("Optional dependency missing (numpy/pandas)")

        compactor = IdCompactor.from_config({"id_compaction": {"enabled": True}}, sep="__", index_key="id")

        with self.assertRaises(Exception) as cm:
            extract_rows_from_jsons(
                [
                    {"id": "https://openalex.org/W1"},
                    {
                        "id": "https://openalex.org/W2",
                        "author_id": "https://openalex.org/A1",
                        "author_openalex_id": "A2",
                    },
                ],
                index_key="id",
                base_table="works",
                id_compactor=compactor,
                parallel_workers=2,
            )

        self.assertIn("collision", str(cm.exception))

    def test_tsv_worker_reports_id_compaction_collision(self):
        if _safe_flatten_jsons_to_tsv_worker is None:
            self.skipTest("Optional dependency missing (numpy/pandas)")

        with tempfile.TemporaryDirectory() as td:
            res = _safe_flatten_jsons_to_tsv_worker(
                (
                    0,
                    [
                        {
                            "id": "https://openalex.org/W1",
                            "author_id": "https://openalex.org/A1",
                            "author_openalex_id": "A2",
                        }
                    ],
                    "id",
                    (),
                    "__",
                    td,
                    None,
                    "works",
                    None,
                    None,
                    False,
                    {"enabled": True, "preset": "openalex", "mode": "semantic_column_strip"},
                )
            )

            self.assertFalse(res["ok"])
            self.assertIn("IdCompactionError", res["error"]["type"])


if __name__ == "__main__":
    unittest.main()
