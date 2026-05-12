import unittest
from unittest.mock import patch

from KISTI_DB_Manager import rust_arrow_backend


class FakeRustExtension:
    def __init__(self):
        self.calls = []

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


if __name__ == "__main__":
    unittest.main()
