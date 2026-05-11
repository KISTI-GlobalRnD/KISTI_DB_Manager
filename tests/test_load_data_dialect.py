import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from KISTI_DB_Manager import load_data


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = conn.rowcount

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.conn.statements.append(sql)
        self.conn.params.append(params)


class _FakeConn:
    def __init__(self, *, rowcount: int):
        self.rowcount = int(rowcount)
        self.statements = []
        self.params = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class TestLoadDataDialect(unittest.TestCase):
    def test_duckdb_and_mysql_dialects_keep_escape_pairing_explicit(self):
        duckdb_copy = load_data.DUCKDB_LOAD_DATA_DIALECT.duckdb_copy_options_sql()
        duckdb_load = load_data.DUCKDB_LOAD_DATA_DIALECT.mysql_fields_lines_sql(line_terminator="\n")

        self.assertIn("QUOTE '\"'", duckdb_copy)
        self.assertIn("ESCAPE '\"'", duckdb_copy)
        self.assertIn("NULLSTR 'NULL'", duckdb_copy)
        self.assertIn("OPTIONALLY ENCLOSED BY '\"'", duckdb_load)
        self.assertIn("ESCAPED BY '\"'", duckdb_load)

    def test_generated_tsv_dialect_uses_backslash_escape_without_csv_quotes(self):
        generated_load = load_data.MYSQL_GENERATED_TSV_DIALECT.mysql_fields_lines_sql(line_terminator="\n")
        expected_mysql_escape = "'" + "\\\\" + "'"

        self.assertNotIn("OPTIONALLY ENCLOSED", generated_load)
        self.assertIn(f"ESCAPED BY {expected_mysql_escape}", generated_load)

    def test_tabular_load_returns_rowcount_and_commits_on_match(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "stage.tsv"
            path.write_text("a\tb\n", encoding="utf-8")
            conn = _FakeConn(rowcount=1)

            loaded = load_data.load_data_local_infile_tabular_file(
                conn=conn,
                table_name="works",
                file_path=str(path),
                sep="\t",
                columns_expr=["`id`", "`title`"],
                ignore_lines=0,
                dialect=load_data.DUCKDB_LOAD_DATA_DIALECT,
                expected_rows=1,
            )

        self.assertEqual(loaded, 1)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(conn.rollbacks, 0)
        self.assertEqual(conn.params[0], (str(path),))
        self.assertIn("LOAD DATA LOCAL INFILE %s", conn.statements[0])
        self.assertIn("OPTIONALLY ENCLOSED BY '\"'", conn.statements[0])
        self.assertIn("ESCAPED BY '\"'", conn.statements[0])

    def test_tabular_load_can_override_line_terminator_for_generated_files(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "stage.tsv"
            path.write_text('a\t"line1\r\nline2"\n', encoding="utf-8", newline="")
            conn = _FakeConn(rowcount=1)

            load_data.load_data_local_infile_tabular_file(
                conn=conn,
                table_name="works",
                file_path=str(path),
                sep="\t",
                columns_expr=["`id`", "`title`"],
                ignore_lines=0,
                dialect=load_data.DUCKDB_LOAD_DATA_DIALECT,
                expected_rows=1,
                line_terminator="\n",
            )

        self.assertIn("LINES TERMINATED BY '\\n'", conn.statements[0])

    def test_tabular_load_rolls_back_before_commit_on_rowcount_mismatch(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "stage.tsv"
            path.write_text("a\tb\n", encoding="utf-8")
            conn = _FakeConn(rowcount=0)

            with self.assertRaisesRegex(RuntimeError, "row count mismatch"):
                load_data.load_data_local_infile_tabular_file(
                    conn=conn,
                    table_name="works",
                    file_path=str(path),
                    sep="\t",
                    columns_expr=["`id`", "`title`"],
                    ignore_lines=0,
                    dialect=load_data.DUCKDB_LOAD_DATA_DIALECT,
                    expected_rows=1,
                )

        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.rollbacks, 1)

    def test_tabular_load_can_emit_ignore_without_expected_row_check(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "stage.tsv"
            path.write_text("a\tb\n", encoding="utf-8")
            conn = _FakeConn(rowcount=0)

            loaded = load_data.load_data_local_infile_tabular_file(
                conn=conn,
                table_name="works",
                file_path=str(path),
                sep="\t",
                columns_expr=["`id`", "`title`"],
                ignore_lines=0,
                dialect=load_data.MYSQL_GENERATED_TSV_DIALECT,
                ignore_duplicates=True,
            )

        self.assertEqual(loaded, 0)
        self.assertEqual(conn.commits, 1)
        self.assertIn("LOAD DATA LOCAL INFILE", conn.statements[0])
        self.assertIn(" IGNORE INTO TABLE `works`", conn.statements[0])


if __name__ == "__main__":
    unittest.main()
