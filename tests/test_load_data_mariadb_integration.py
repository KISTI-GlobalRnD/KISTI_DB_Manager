import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from KISTI_DB_Manager import openalex_materialize as oa_materialize
from KISTI_DB_Manager import load_data


def _integration_db_config():
    if os.environ.get("KISTI_LOAD_DATA_INTEGRATION") != "1":
        return None

    def env(*names, default=""):
        for name in names:
            value = os.environ.get(name)
            if value:
                return value
        return default

    cfg = {
        "host": env("KISTI_TEST_DB_HOST", "KISTI_SMOKE_DB_HOST", default="127.0.0.1"),
        "port": int(env("KISTI_TEST_DB_PORT", "KISTI_SMOKE_DB_PORT", default="3307")),
        "user": env("KISTI_TEST_DB_USER", "KISTI_SMOKE_DB_USER", default="root"),
        "password": env("KISTI_TEST_DB_PASSWORD", "KISTI_SMOKE_DB_PASSWORD", default="rootpass"),
        "database": env("KISTI_TEST_DB_NAME", "KISTI_SMOKE_DB_NAME", default="kisti_smoke"),
    }
    if not cfg["database"]:
        return None
    return cfg


def _connect_or_skip(testcase):
    cfg = _integration_db_config()
    if cfg is None:
        testcase.skipTest("set KISTI_LOAD_DATA_INTEGRATION=1 and KISTI_TEST_DB_* or KISTI_SMOKE_DB_* to run")
    try:
        import pymysql
    except Exception as exc:  # pragma: no cover
        testcase.skipTest(f"pymysql is required: {exc}")
    try:
        return pymysql.connect(
            host=cfg["host"],
            port=int(cfg["port"]),
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            charset="utf8mb4",
            local_infile=True,
            autocommit=False,
        )
    except Exception as exc:  # pragma: no cover
        testcase.skipTest(f"MariaDB integration DB is unavailable: {exc}")


def _qi(ident: str) -> str:
    return str(ident).replace("`", "``")


class TestLoadDataMariaDbIntegration(unittest.TestCase):
    def setUp(self):
        self.conn = _connect_or_skip(self)
        self.table = f"kisti_load_data_it_{uuid4().hex[:12]}"
        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{_qi(self.table)}`")
            cur.execute(
                f"CREATE TABLE `{_qi(self.table)}` ("
                "`id` VARCHAR(32) NOT NULL PRIMARY KEY, "
                "`txt` LONGTEXT NULL"
                ") CHARACTER SET utf8mb4"
            )
        self.conn.commit()

    def tearDown(self):
        if getattr(self, "conn", None) is None:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{_qi(self.table)}`")
            self.conn.commit()
        finally:
            self.conn.close()

    def _write_duckdb_stage(self, path: Path, rows: list[tuple[str, str | None]]) -> None:
        try:
            import duckdb
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"duckdb is required: {exc}")

        con = duckdb.connect(database=":memory:")
        try:
            con.execute("CREATE TABLE stage(id VARCHAR, txt VARCHAR)")
            con.executemany("INSERT INTO stage VALUES (?, ?)", rows)
            con.execute(
                f"COPY stage TO {json.dumps(str(path))} "
                f"{load_data.DUCKDB_LOAD_DATA_DIALECT.duckdb_copy_options_sql()};"
            )
        finally:
            con.close()

    def _fetch_rows(self):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT `id`, `txt` FROM `{_qi(self.table)}` ORDER BY `id`")
            return list(cur.fetchall())

    def test_duckdb_stage_round_trips_quoted_newlines_quotes_backslashes_and_nulls(self):
        rows = [
            ("W1", "plain"),
            ("W2", 'line1 "\nline2'),
            ("W3", 'quote "inside"'),
            ("W4", None),
            ("W5", 'backslash quote \\"\ninside'),
            ("W6", "NULL"),
            ("W7", "line1\r\nline2"),
        ]
        with TemporaryDirectory() as td:
            path = Path(td) / "duckdb_stage.tsv"
            self._write_duckdb_stage(path, rows)

            loaded = load_data.load_data_local_infile_tabular_file(
                conn=self.conn,
                table_name=self.table,
                file_path=str(path),
                sep="\t",
                columns_expr=["`id`", "`txt`"],
                ignore_lines=0,
                dialect=load_data.DUCKDB_LOAD_DATA_DIALECT,
                expected_rows=len(rows),
                line_terminator="\n",
            )

        self.assertEqual(loaded, len(rows))
        self.assertEqual(self._fetch_rows(), rows)

    def test_wrong_escape_dialect_rolls_back_on_rowcount_mismatch_or_parse_error(self):
        rows = [
            ("W1", "before"),
            ("W2", 'backslash quote \\"\nthen text'),
            ("W3", "after"),
        ]
        wrong_dialect = load_data.LoadDataDialect(
            name="old_wrong_duckdb_reader",
            delimiter="\t",
            nullstr=r"\N",
            quote='"',
            escape="\\",
            header=False,
        )

        with TemporaryDirectory() as td:
            path = Path(td) / "duckdb_stage.tsv"
            self._write_duckdb_stage(path, rows)

            with self.assertRaises(Exception):
                load_data.load_data_local_infile_tabular_file(
                    conn=self.conn,
                    table_name=self.table,
                    file_path=str(path),
                    sep="\t",
                    columns_expr=["`id`", "`txt`"],
                    ignore_lines=0,
                    dialect=wrong_dialect,
                    expected_rows=len(rows),
                    line_terminator="\n",
                )

        self.assertEqual(self._fetch_rows(), [])

    def test_ignore_duplicates_loads_unique_rows_without_expected_row_check(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "generated.tsv"
            path.write_text("W1\tfirst\nW1\tduplicate\nW2\tsecond\n", encoding="utf-8", newline="\n")

            loaded = load_data.load_data_local_infile_tabular_file(
                conn=self.conn,
                table_name=self.table,
                file_path=str(path),
                sep="\t",
                columns_expr=["`id`", "`txt`"],
                ignore_lines=0,
                dialect=load_data.MYSQL_GENERATED_TSV_DIALECT,
                ignore_duplicates=True,
                line_terminator="\n",
            )

        self.assertEqual(loaded, 2)
        self.assertEqual(self._fetch_rows(), [("W1", "first"), ("W2", "second")])

    def test_direct_materialize_preflight_validates_target_runtime_dialect(self):
        with TemporaryDirectory() as td:
            oa_materialize._run_duckdb_load_data_preflight_on_conn(
                conn=self.conn,
                staging_dir=td,
                report=None,
            )


if __name__ == "__main__":
    unittest.main()
