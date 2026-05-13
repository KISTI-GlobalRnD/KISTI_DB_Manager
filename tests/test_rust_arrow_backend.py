import unittest
from unittest.mock import patch

from KISTI_DB_Manager import rust_arrow_backend


class FakeRustExtension:
    def __init__(self):
        self.calls = []

    def persist_json_lines_batch(self, lines, options):
        self.calls.append((lines, options))
        return {"ok": True, "records_ok": len(lines), "records_failed": 0, "tables": []}

    def persist_jsonl_sources(self, sources, options):
        self.calls.append((sources, options))
        return {"ok": True, "records_read": 0, "records_ok": 0, "records_failed": 0, "tables": []}

    def load_parquet_files_to_mysql(self, payload, options):
        self.calls.append((payload, options))
        return {"ok": True, "files_loaded": len(payload), "tables_loaded": len(payload), "rows_loaded": 0}


class TestRustArrowBackendWrapper(unittest.TestCase):
    def test_rust_mysql_loader_passes_transaction_default(self):
        ext = FakeRustExtension()
        with patch("KISTI_DB_Manager.rust_arrow_backend._load_extension", return_value=ext):
            res = rust_arrow_backend.load_parquet_files_to_mysql(
                [
                    {
                        "path": "/tmp/x.parquet",
                        "table_sql": "t",
                        "columns_original": ["id"],
                        "columns_sql": ["id"],
                    }
                ],
                db_config={"host": "h", "user": "u", "password": "p", "database": "d"},
                batch_size=123,
            )

        self.assertTrue(res["ok"])
        payload, options = ext.calls[0]
        self.assertEqual(payload[0]["table_sql"], "t")
        self.assertEqual(options["batch_size"], 123)
        self.assertEqual(options["transaction"], True)

    def test_rust_mysql_loader_can_disable_transaction(self):
        ext = FakeRustExtension()
        with patch("KISTI_DB_Manager.rust_arrow_backend._load_extension", return_value=ext):
            rust_arrow_backend.load_parquet_files_to_mysql(
                [
                    {
                        "path": "/tmp/x.parquet",
                        "table_sql": "t",
                        "columns_original": ["id"],
                        "columns_sql": ["id"],
                    }
                ],
                db_config={"host": "h", "user": "u", "password": "p", "database": "d"},
                transaction=False,
            )

        self.assertEqual(ext.calls[0][1]["transaction"], False)

    def test_raw_jsonl_wrapper_passes_lines_and_options(self):
        ext = FakeRustExtension()
        input_lines = [b'{"id": 1}', '{"id": 2}']
        with patch("KISTI_DB_Manager.rust_arrow_backend._load_extension", return_value=ext):
            res = rust_arrow_backend.persist_json_lines_batch_to_parquet(
                input_lines,
                base_table="base",
                index_key="id",
                except_keys=["items"],
                excepted_expand_dict=False,
                sep="__",
                parquet_dir="/tmp/parquet",
                batch_idx=3,
                index_offset=10,
                record_contexts=[{"line_no": 1}, {"line_no": 2}],
                parallel_workers=2,
            )

        self.assertTrue(res["ok"])
        lines, options = ext.calls[0]
        self.assertIs(lines, input_lines)
        self.assertEqual(lines, [b'{"id": 1}', '{"id": 2}'])
        self.assertEqual(options["base_table"], "base")
        self.assertEqual(options["except_keys"], ["items"])
        self.assertEqual(options["batch_idx"], 3)
        self.assertEqual(options["index_offset"], 10)
        self.assertEqual(options["parallel_workers"], 2)

    def test_direct_jsonl_source_wrapper_passes_sources_and_options(self):
        ext = FakeRustExtension()
        with patch("KISTI_DB_Manager.rust_arrow_backend._load_extension", return_value=ext):
            res = rust_arrow_backend.persist_jsonl_sources_to_parquet(
                ["/tmp/a.jsonl"],
                base_table="base",
                index_key="id",
                except_keys=[],
                excepted_expand_dict=False,
                sep="__",
                parquet_dir="/tmp/parquet",
                batch_idx=4,
                index_offset=11,
                parallel_workers=3,
                chunk_size=99,
                max_records=123,
            )

        self.assertTrue(res["ok"])
        sources, options = ext.calls[0]
        self.assertEqual(sources, ["/tmp/a.jsonl"])
        self.assertEqual(options["batch_idx"], 4)
        self.assertEqual(options["index_offset"], 11)
        self.assertEqual(options["parallel_workers"], 3)
        self.assertEqual(options["chunk_size"], 99)
        self.assertEqual(options["max_records"], 123)


if __name__ == "__main__":
    unittest.main()
