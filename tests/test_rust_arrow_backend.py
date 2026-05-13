import unittest
from unittest.mock import patch

from KISTI_DB_Manager import rust_arrow_backend


class FakeRustExtension:
    def __init__(self):
        self.calls = []

    def persist_json_lines_batch(self, lines, options):
        self.calls.append((lines, options))
        return {"ok": True, "records_ok": len(lines), "records_failed": 0, "tables": []}

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
        with patch("KISTI_DB_Manager.rust_arrow_backend._load_extension", return_value=ext):
            res = rust_arrow_backend.persist_json_lines_batch_to_parquet(
                [b'{"id": 1}', '{"id": 2}'],
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
        self.assertEqual(lines, [b'{"id": 1}', '{"id": 2}'])
        self.assertEqual(options["base_table"], "base")
        self.assertEqual(options["except_keys"], ["items"])
        self.assertEqual(options["batch_idx"], 3)
        self.assertEqual(options["index_offset"], 10)
        self.assertEqual(options["parallel_workers"], 2)


if __name__ == "__main__":
    unittest.main()
