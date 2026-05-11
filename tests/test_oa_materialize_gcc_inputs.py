import importlib.util
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from KISTI_DB_Manager import load_data


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "oa_materialize_gcc_inputs.py"
SPEC = importlib.util.spec_from_file_location("oa_materialize_gcc_inputs", SCRIPT_PATH)
oa_gcc = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(oa_gcc)


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = conn.rowcount

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.conn.calls.append(sql)
        self.conn.params.append(params)


class _FakeConn:
    def __init__(self, *, rowcount: int):
        self.rowcount = int(rowcount)
        self.calls = []
        self.params = []

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass


class TestOaMaterializeGccInputs(unittest.TestCase):
    def test_tsv_escape_reuses_common_mysql_escape_rules(self):
        self.assertEqual(oa_gcc._tsv_escape(None), r"\N")
        self.assertEqual(oa_gcc._tsv_escape(True), "1")
        self.assertEqual(oa_gcc._tsv_escape(False), "0")
        self.assertEqual(
            oa_gcc._tsv_escape("a\tb\nc\\d"),
            load_data.mysql_escape_load_data_value("a\tb\nc\\d"),
        )

    def test_load_tsv_uses_common_generated_tsv_dialect(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "batch.tsv"
            path.write_text("W1\tTitle\n", encoding="utf-8")
            conn = _FakeConn(rowcount=7)

            loaded = oa_gcc._load_tsv(conn, path=path, table="openalex_works_text", columns=["work_id", "title"])

        self.assertEqual(loaded, 7)
        sql = conn.calls[0]
        self.assertIn("IGNORE INTO TABLE `openalex_works_text`", sql)
        self.assertIn("FIELDS TERMINATED BY '\\t'", sql)
        self.assertIn("ESCAPED BY '\\\\'", sql)
        self.assertIn("LINES TERMINATED BY '\\n'", sql)
        self.assertNotIn("OPTIONALLY ENCLOSED", sql)
        self.assertEqual(conn.params[0], (str(path),))


if __name__ == "__main__":
    unittest.main()
