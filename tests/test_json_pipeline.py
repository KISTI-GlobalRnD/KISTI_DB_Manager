import unittest
from unittest.mock import patch

import json
import os
import tempfile
import types
import sys
from pathlib import Path


from KISTI_DB_Manager.pipeline import _json_loads_factory, run_json_pipeline
from KISTI_DB_Manager.processing import extract_rows_from_jsons, flatten_nested_json_with_list


class DummyDF:
    def __init__(self, columns, rows=1):
        self.columns = list(columns)
        self._rows = int(rows)

    def __len__(self):
        return self._rows

    def reset_index(self, drop=True):
        return self

    def to_parquet(self, path, *args, **kwargs):
        Path(path).write_text("dummy parquet", encoding="utf-8")


class TestJsonPipeline(unittest.TestCase):
    def _require_pyarrow(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except Exception as e:  # pragma: no cover - optional dependency guard
            self.skipTest(f"pyarrow unavailable: {e}")
        return pa, pq

    def _require_rust_arrow_with_pyarrow(self):
        pa, pq = self._require_pyarrow()

        from KISTI_DB_Manager.rust_arrow_backend import rust_arrow_available

        if not rust_arrow_available():
            self.skipTest("rust-arrow backend extension unavailable")
        return pa, pq

    def _require_simd_json_parser(self):
        self._require_rust_arrow_with_pyarrow()

        from KISTI_DB_Manager.rust_arrow_backend import persist_json_lines_batch_to_parquet

        with tempfile.TemporaryDirectory() as td:
            try:
                persist_json_lines_batch_to_parquet(
                    ['{"id": 1}'],
                    base_table="base",
                    index_key="id",
                    except_keys=[],
                    excepted_expand_dict=False,
                    sep="__",
                    parquet_dir=Path(td),
                    batch_idx=0,
                    index_offset=0,
                    record_contexts=[{"line_no": 1}],
                    parallel_workers=0,
                    parser_backend="simd-json",
                )
            except Exception as exc:
                if "simd-json Cargo feature" in str(exc):
                    self.skipTest("simd-json parser feature unavailable")
                raise

    def test_json_loads_factory_preserves_large_integer_literals(self):
        loads = _json_loads_factory()
        huge_positive = 18446744073709551616
        huge_negative = -9223372036854775809

        parsed = loads(
            (
                '{"positive": %d, "negative": %d, '
                '"quoted": "18446744073709551616"}'
            )
            % (huge_positive, huge_negative)
        )

        self.assertEqual(parsed["positive"], huge_positive)
        self.assertIs(type(parsed["positive"]), int)
        self.assertEqual(parsed["negative"], huge_negative)
        self.assertIs(type(parsed["negative"]), int)
        self.assertEqual(parsed["quoted"], "18446744073709551616")

    def test_extract_rows_preserves_scalar_items_in_mixed_object_list(self):
        rows, subs, excepted = extract_rows_from_jsons(
            [{"id": 1, "items": [{"a": 1}, "lost"]}],
            index_key="id",
            base_table="base",
        )

        self.assertEqual(rows, [{"id": 1}])
        self.assertEqual(excepted, {})
        self.assertEqual(subs["items"], [{"id": 1, "items__a": 1}, {"id": 1, "items": "lost"}])

        legacy_main, legacy_subs, legacy_excepted = flatten_nested_json_with_list(
            {"id": 1, "items": [{"a": 1}, "lost"]},
            index_key="id",
            index=0,
        )
        self.assertEqual(legacy_main.to_dict("records"), [{"id": 1}])
        self.assertEqual(legacy_excepted, {})
        self.assertIn("items", legacy_subs)
        self.assertIn("items", legacy_subs["items"].columns)
        self.assertNotIn("items__", legacy_subs["items"].columns)
        self.assertEqual(legacy_subs["items"].to_dict("records")[1]["items"], "lost")

    def _read_single_parquet_table(self, pq_module, root: Path, table: str):
        paths = sorted((root / table).glob("*.parquet"))
        self.assertEqual(len(paths), 1, [p.name for p in paths])
        return pq_module.read_table(paths[0])

    def test_run_json_pipeline_handles_missing_processing_backend(self):
        # Ensure the import path is exercised even when optional deps are installed
        # and earlier tests have already imported the processing backend.
        import sys

        sys.modules.pop("KISTI_DB_Manager.processing", None)

        data_config = {
            "PATH": "data/",
            "file_name": "x.jsonl",
            "file_type": "jsonl",
            "table_name": "base",
            "KEY_SEP": "__",
            "persist_parquet_files": False,
        }
        db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

        real_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            # Relative import inside the package can come through as `name="processing", level=1`.
            if name.endswith(".processing") or name in {"KISTI_DB_Manager.processing", "processing"}:
                raise ModuleNotFoundError("No module named 'numpy'")
            return real_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=fake_import):
            res = run_json_pipeline(
                data_config,
                db_config,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

        self.assertEqual(res.name_maps, {})
        self.assertGreaterEqual(len(res.report.issues), 1)

    def test_run_json_pipeline_builds_and_extends_namemaps(self):
        data_config = {
            "PATH": "data/",
            "file_name": "x.jsonl",
            "file_type": "jsonl",
            "table_name": "base",
            "KEY_SEP": "__",
            "persist_parquet_files": False,
        }
        db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

        def fake_iter_records(_dc, report=None, max_records=None, with_context=False):
            yield {"id": 1}
            yield {"id": 2}
            yield {"id": 3}

        calls = []

        def fake_extract(batch_records, **_kwargs):
            calls.append(len(batch_records))
            if len(calls) == 1:
                main = DummyDF(["id", "a"], rows=len(batch_records))
            else:
                main = DummyDF(["id", "a", "b"], rows=len(batch_records))
            subs = {"items": DummyDF(["id", "items__x"], rows=1)}
            return main, subs, {}

        with patch("KISTI_DB_Manager.pipeline._iter_json_records", side_effect=fake_iter_records), patch(
            "KISTI_DB_Manager.manage.create_table_from_columns",
            side_effect=lambda *_a, **kw: kw.get("name_map"),
        ) as p_create, patch(
            "KISTI_DB_Manager.manage.fill_table_from_dataframe",
            side_effect=lambda *_a, **kw: kw.get("name_map"),
        ) as p_load:
            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=2,
                extract_fn=fake_extract,
                create=True,
                load=True,
                index=False,
                optimize=False,
                continue_on_error=False,
            )

        self.assertEqual(calls, [2, 1])
        self.assertIn("base", res.name_maps)
        self.assertIn("base__items", res.name_maps)
        self.assertIn("b", res.name_maps["base"].columns_original)
        self.assertIn("name_maps_json", res.report.artifacts)
        self.assertEqual(p_create.call_count, 2)  # base + base__items
        self.assertEqual(p_load.call_count, 4)  # 2 batches * 2 tables

    def test_run_json_pipeline_auto_except_detects_high_cardinality_dict_path(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p = root / "x.jsonl"
            lines = []
            for i in range(12):
                payload = {"id": i + 1, "high_map": {f"k_{i}_{j}": j for j in range(3)}, "stable": {"a": 1, "b": 2}}
                lines.append(str(payload).replace("'", '"'))
            p.write_text("\n".join(lines) + "\n", encoding="utf-8")

            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "auto_except": True,
                "auto_except_sample_records": 12,
                "auto_except_sample_max_sources": 1,
                "auto_except_seed": 7,
                "auto_except_unique_key_threshold": 10,
                "auto_except_min_observations": 5,
                "auto_except_novelty_threshold": 1.0,
                "persist_parquet_files": False,
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            seen_except_keys: list[list[str]] = []

            def fake_extract(batch_records, **kwargs):
                seen_except_keys.append(list(kwargs.get("except_keys") or []))
                return DummyDF(["id"], rows=len(batch_records)), {}, {}

            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=50,
                extract_fn=fake_extract,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=False,
            )

            self.assertTrue(seen_except_keys)
            self.assertIn("high_map", seen_except_keys[0])
            auto_meta = (res.report.artifacts or {}).get("auto_except") or {}
            self.assertEqual(auto_meta.get("enabled"), True)
            self.assertIn("high_map", auto_meta.get("detected_except_keys") or [])

    def test_run_json_pipeline_persists_tsv_artifacts_without_db_load(self):
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "tsv_out"
            data_config = {
                "PATH": str(Path(td)),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": True,
                "db_load_method": "auto",
                "persist_parquet_files": False,
                "persist_tsv_files": True,
                "persist_tsv_dir": str(out_dir),
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def fake_iter_records(_dc, report=None, max_records=None, with_context=False):
                yield {"id": 1, "x": "a"}

            def fake_worker(args):
                tmp_dir = str(args[5] or td)
                workdir_token = str(args[12] if len(args) > 12 else "test")
                wd = tempfile.mkdtemp(prefix=f"kisti_flatten_{workdir_token}_", dir=tmp_dir)
                p = Path(wd) / "main.tsv"
                p.write_text("1\ta\n", encoding="utf-8")
                return {
                    "ok": True,
                    "index_offset": 0,
                    "records_ok": 1,
                    "records_failed": 0,
                    "errors": [],
                    "timings_ms": {"flatten_ms": 1, "tsv_write_ms": 1},
                    "workdir": str(wd),
                    "main": {"path": str(p), "columns": ["id", "x"], "rows": 1},
                    "subs": {},
                    "excepted": {},
                }

            fake_processing = types.SimpleNamespace(
                extract_data_from_jsons=lambda *args, **kwargs: None,
                extract_rows_from_jsons=lambda *args, **kwargs: None,
                _safe_flatten_jsons_to_tsv_worker=fake_worker,
            )

            with patch("KISTI_DB_Manager.pipeline._iter_json_records", side_effect=fake_iter_records), patch.dict(
                sys.modules,
                {"KISTI_DB_Manager.processing": fake_processing},
            ):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            persisted = sorted(out_dir.rglob("*.tsv"))
            self.assertEqual(len(persisted), 1)
            self.assertEqual((res.report.stats or {}).get("tsv_files_persisted"), 1)
            self.assertEqual((res.report.stats or {}).get("rows_emitted"), 1)

    def test_run_json_pipeline_tsv_rejects_symlink_root_after_missing_parent_dotdot(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            external = root / "external"
            external.mkdir()
            out_link = root / "tsv_link"
            out_link.symlink_to(external, target_is_directory=True)
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": True,
                "db_load_method": "auto",
                "persist_parquet_files": False,
                "persist_tsv_files": True,
                "persist_tsv_dir": str(root / "missing_parent" / ".." / "tsv_link"),
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertTrue(any(issue.stage == "json_pipeline.tsv_persist" for issue in res.report.issues))
            self.assertEqual(list(external.iterdir()), [])

    def test_run_json_pipeline_tsv_skips_symlink_output_file(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "tsv_out"
            table_dir = out_dir / "base"
            table_dir.mkdir(parents=True)
            target = root / "external.tsv"
            target.write_text("external", encoding="utf-8")
            (table_dir / "b000000_flatten_main.tsv").symlink_to(target)
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": True,
                "db_load_method": "auto",
                "persist_parquet_files": False,
                "persist_tsv_files": True,
                "persist_tsv_dir": str(out_dir),
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def fake_iter_records(_dc, report=None, max_records=None, with_context=False):
                yield {"id": 1, "x": "a"}

            def fake_worker(args):
                tmp_dir = str(args[5] or td)
                workdir_token = str(args[12] if len(args) > 12 else "test")
                wd = tempfile.mkdtemp(prefix=f"kisti_flatten_{workdir_token}_", dir=tmp_dir)
                p = Path(wd) / "main.tsv"
                p.write_text("1\ta\n", encoding="utf-8")
                return {
                    "ok": True,
                    "index_offset": 0,
                    "records_ok": 1,
                    "records_failed": 0,
                    "errors": [],
                    "timings_ms": {"flatten_ms": 1, "tsv_write_ms": 1},
                    "workdir": str(wd),
                    "main": {"path": str(p), "columns": ["id", "x"], "rows": 1},
                    "subs": {},
                    "excepted": {},
                }

            fake_processing = types.SimpleNamespace(
                extract_data_from_jsons=lambda *args, **kwargs: None,
                extract_rows_from_jsons=lambda *args, **kwargs: None,
                _safe_flatten_jsons_to_tsv_worker=fake_worker,
            )

            with patch("KISTI_DB_Manager.pipeline._iter_json_records", side_effect=fake_iter_records), patch.dict(
                sys.modules,
                {"KISTI_DB_Manager.processing": fake_processing},
            ):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            self.assertFalse(res.report.issues)
            self.assertEqual(target.read_text(encoding="utf-8"), "external")
            self.assertTrue((table_dir / "b000000_flatten_main.tsv").is_symlink())
            self.assertEqual((table_dir / "b000000_flatten_main_1.tsv").read_text(encoding="utf-8"), "1\ta\n")
            self.assertEqual((res.report.stats or {}).get("tsv_files_persisted"), 1)

    def test_run_json_pipeline_tsv_ignores_unsafe_worker_workdir_without_deleting_it(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "tsv_out"
            victim_dir = root / "victim"
            victim_dir.mkdir()
            sentinel = victim_dir / "sentinel.txt"
            sentinel.write_text("keep", encoding="utf-8")
            worker_file = victim_dir / "main.tsv"
            worker_file.write_text("1\ta\n", encoding="utf-8")
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": True,
                "db_load_method": "auto",
                "persist_parquet_files": False,
                "persist_tsv_files": True,
                "persist_tsv_dir": str(out_dir),
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def fake_iter_records(_dc, report=None, max_records=None, with_context=False):
                yield {"id": 1, "x": "a"}

            def fake_worker(args):
                return {
                    "ok": True,
                    "index_offset": 0,
                    "records_ok": 1,
                    "records_failed": 0,
                    "errors": [],
                    "timings_ms": {"flatten_ms": 1, "tsv_write_ms": 1},
                    "workdir": str(victim_dir),
                    "main": {"path": str(worker_file), "columns": ["id", "x"], "rows": 1},
                    "subs": {},
                    "excepted": {},
                }

            fake_processing = types.SimpleNamespace(
                extract_data_from_jsons=lambda *args, **kwargs: None,
                extract_rows_from_jsons=lambda *args, **kwargs: None,
                _safe_flatten_jsons_to_tsv_worker=fake_worker,
            )

            with patch("KISTI_DB_Manager.pipeline._iter_json_records", side_effect=fake_iter_records), patch.dict(
                sys.modules,
                {"KISTI_DB_Manager.processing": fake_processing},
            ):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=True,
                )

            stages = {issue.stage for issue in res.report.issues}
            self.assertIn("json_pipeline.flatten.parallel_tsv.workdir", stages)
            self.assertIn("json_pipeline.flatten.parallel_tsv.file", stages)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")
            self.assertEqual(worker_file.read_text(encoding="utf-8"), "1\ta\n")
            self.assertFalse(list(out_dir.rglob("*.tsv")) if out_dir.exists() else [])

    def test_safe_flatten_tsv_worker_cleans_workdir_on_write_failure(self):
        from KISTI_DB_Manager.processing import _safe_flatten_jsons_to_tsv_worker

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "kisti_flatten_test_manual"

            def fake_mkdtemp(prefix, dir):
                self.assertEqual(prefix, "kisti_flatten_test_")
                self.assertEqual(Path(dir), root)
                workdir.mkdir()
                return str(workdir)

            real_open = open

            def failing_open(path, *args, **kwargs):
                if str(path).startswith(str(workdir)):
                    raise OSError("write failed")
                return real_open(path, *args, **kwargs)

            with patch("tempfile.mkdtemp", side_effect=fake_mkdtemp), patch(
                "builtins.open",
                side_effect=failing_open,
            ):
                res = _safe_flatten_jsons_to_tsv_worker(
                    (
                        0,
                        [{"id": 1, "x": "a"}],
                        "id",
                        (),
                        "__",
                        str(root),
                        None,
                        "base",
                        None,
                        None,
                        False,
                        None,
                        "test",
                    )
                )

            self.assertFalse(res.get("ok"))
            self.assertFalse(workdir.exists())

    def test_run_json_pipeline_tsv_merge_parent_temp_file_is_finalized_safely(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                "\n".join(
                    json.dumps({"id": i, "x": f"v{i}", "items": [{"n": i}]})
                    for i in range(6)
                )
                + "\n",
                encoding="utf-8",
            )
            loaded_paths: list[Path] = []
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": True,
                "db_load_method": "auto",
                "parallel_workers": 2,
                "tmp_dir": str(root),
                "persist_parquet_files": False,
                "persist_tsv_files": False,
            }

            class FakeCursor:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def execute(self, *_args, **_kwargs):
                    return None

                def fetchone(self):
                    return (1,)

            class FakeConnection:
                def cursor(self):
                    return FakeCursor()

                def commit(self):
                    return None

                def rollback(self):
                    return None

                def close(self):
                    return None

            def fake_load_tsv(file_path, *args, **kwargs):
                loaded_paths.append(Path(file_path))
                return kwargs.get("name_map")

            with patch("pymysql.connect", return_value=FakeConnection()), patch(
                "KISTI_DB_Manager.manage.fill_table_from_tsv_file",
                side_effect=fake_load_tsv,
            ):
                res = run_json_pipeline(
                    data_config,
                    {"host": "h", "user": "u", "password": "p", "database": "d"},
                    chunk_size=6,
                    create=False,
                    load=True,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            stages = {issue.stage for issue in res.report.issues}
            self.assertNotIn("json_pipeline.flatten.parallel_tsv.file", stages)
            self.assertNotIn("json_pipeline.tsv_persist", stages)
            self.assertTrue(any(path.name.startswith("kisti_merge_") for path in loaded_paths))
            self.assertFalse(list(root.glob("kisti_merge_*.tsv")))
            self.assertFalse([p for p in root.iterdir() if p.is_dir() and p.name.startswith("kisti_flatten_")])

    def test_run_json_pipeline_tsv_merge_parent_temp_files_cleaned_on_load_failure(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                "\n".join(
                    json.dumps({"id": i, "x": f"v{i}", "items": [{"n": i}]})
                    for i in range(6)
                )
                + "\n",
                encoding="utf-8",
            )
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": True,
                "db_load_method": "auto",
                "parallel_workers": 2,
                "tmp_dir": str(root),
                "persist_parquet_files": False,
                "persist_tsv_files": False,
            }

            class FakeCursor:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def execute(self, *_args, **_kwargs):
                    return None

                def fetchone(self):
                    return (1,)

            class FakeConnection:
                def cursor(self):
                    return FakeCursor()

                def commit(self):
                    return None

                def rollback(self):
                    return None

                def close(self):
                    return None

            def fail_load_tsv(*_args, **_kwargs):
                raise RuntimeError("load failed")

            with patch("pymysql.connect", return_value=FakeConnection()), patch(
                "KISTI_DB_Manager.manage.fill_table_from_tsv_file",
                side_effect=fail_load_tsv,
            ):
                with self.assertRaises(RuntimeError):
                    run_json_pipeline(
                        data_config,
                        {"host": "h", "user": "u", "password": "p", "database": "d"},
                        chunk_size=6,
                        create=False,
                        load=True,
                        index=False,
                        optimize=False,
                        continue_on_error=False,
                    )

            self.assertFalse(list(root.glob("kisti_merge_*.tsv")))
            self.assertFalse(list(root.glob("kisti_union_*.tsv")))
            self.assertFalse([p for p in root.iterdir() if p.is_dir() and p.name.startswith("kisti_flatten_")])

    def test_run_json_pipeline_persist_tsv_files_survive_load_failure(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "tsv_out"
            (root / "x.jsonl").write_text(
                "\n".join(
                    json.dumps({"id": i, "x": f"v{i}", "items": [{"n": i}]})
                    for i in range(6)
                )
                + "\n",
                encoding="utf-8",
            )
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": True,
                "db_load_method": "auto",
                "parallel_workers": 2,
                "tmp_dir": str(root),
                "persist_parquet_files": False,
                "persist_tsv_files": True,
                "persist_tsv_dir": str(out_dir),
            }

            class FakeCursor:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def execute(self, *_args, **_kwargs):
                    return None

                def fetchone(self):
                    return (1,)

            class FakeConnection:
                def cursor(self):
                    return FakeCursor()

                def commit(self):
                    return None

                def rollback(self):
                    return None

                def close(self):
                    return None

            def fail_load_tsv(*_args, **_kwargs):
                raise RuntimeError("load failed")

            with patch("pymysql.connect", return_value=FakeConnection()), patch(
                "KISTI_DB_Manager.manage.fill_table_from_tsv_file",
                side_effect=fail_load_tsv,
            ):
                with self.assertRaises(RuntimeError):
                    run_json_pipeline(
                        data_config,
                        {"host": "h", "user": "u", "password": "p", "database": "d"},
                        chunk_size=6,
                        create=False,
                        load=True,
                        index=False,
                        optimize=False,
                        continue_on_error=False,
                    )

            persisted = sorted(p.relative_to(out_dir) for p in out_dir.rglob("*.tsv"))
            self.assertEqual(len(persisted), 2)
            self.assertTrue(any(str(path).startswith("base/") for path in persisted))
            self.assertTrue(any(str(path).startswith("base__items/") for path in persisted))
            self.assertFalse(list(root.glob("kisti_merge_*.tsv")))
            self.assertFalse(list(root.glob("kisti_union_*.tsv")))
            self.assertFalse([p for p in root.iterdir() if p.is_dir() and p.name.startswith("kisti_flatten_")])

    def test_run_json_pipeline_overlap_batches_tsv_progress_without_parquet_progress(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                "\n".join(
                    json.dumps({"id": i, "x": f"v{i}", "items": [{"n": i}]})
                    for i in range(8)
                )
                + "\n",
                encoding="utf-8",
            )
            loaded_paths: list[Path] = []
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": True,
                "db_load_method": "auto",
                "parallel_workers": 2,
                "db_load_parallel_tables": 2,
                "overlap_batches": True,
                "tmp_dir": str(root),
                "persist_parquet_files": False,
                "persist_tsv_files": False,
            }

            class FakeCursor:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def execute(self, *_args, **_kwargs):
                    return None

                def fetchone(self):
                    return (1,)

            class FakeConnection:
                def cursor(self):
                    return FakeCursor()

                def commit(self):
                    return None

                def rollback(self):
                    return None

                def close(self):
                    return None

            def fake_load_tsv(file_path, *args, **kwargs):
                loaded_paths.append(Path(file_path))
                return kwargs.get("name_map")

            with patch("pymysql.connect", return_value=FakeConnection()), patch(
                "KISTI_DB_Manager.manage.fill_table_from_tsv_file",
                side_effect=fake_load_tsv,
            ):
                res = run_json_pipeline(
                    data_config,
                    {"host": "h", "user": "u", "password": "p", "database": "d"},
                    chunk_size=4,
                    create=False,
                    load=True,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            self.assertFalse(res.report.issues)
            self.assertTrue(loaded_paths)
            self.assertFalse(list(root.glob("kisti_merge_*.tsv")))
            self.assertFalse([p for p in root.iterdir() if p.is_dir() and p.name.startswith("kisti_flatten_")])

    def test_run_json_pipeline_overlap_tsv_temp_files_cleaned_on_load_failure(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                "\n".join(
                    json.dumps({"id": i, "x": f"v{i}", "items": [{"n": i}]})
                    for i in range(8)
                )
                + "\n",
                encoding="utf-8",
            )
            attempted_paths: list[Path] = []
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": True,
                "db_load_method": "auto",
                "parallel_workers": 2,
                "db_load_parallel_tables": 2,
                "overlap_batches": True,
                "tmp_dir": str(root),
                "persist_parquet_files": False,
                "persist_tsv_files": False,
            }

            class FakeCursor:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def execute(self, *_args, **_kwargs):
                    return None

                def fetchone(self):
                    return (1,)

            class FakeConnection:
                def cursor(self):
                    return FakeCursor()

                def commit(self):
                    return None

                def rollback(self):
                    return None

                def close(self):
                    return None

            def fail_load_tsv(file_path, *_args, **_kwargs):
                attempted_paths.append(Path(file_path))
                raise RuntimeError("load failed")

            with patch("pymysql.connect", return_value=FakeConnection()), patch(
                "KISTI_DB_Manager.manage.fill_table_from_tsv_file",
                side_effect=fail_load_tsv,
            ):
                with self.assertRaises(RuntimeError):
                    run_json_pipeline(
                        data_config,
                        {"host": "h", "user": "u", "password": "p", "database": "d"},
                        chunk_size=4,
                        create=False,
                        load=True,
                        index=False,
                        optimize=False,
                        continue_on_error=False,
                    )

            self.assertGreaterEqual(len(attempted_paths), 2)
            self.assertFalse(list(root.glob("kisti_merge_*.tsv")))
            self.assertFalse(list(root.glob("kisti_union_*.tsv")))
            self.assertFalse([p for p in root.iterdir() if p.is_dir() and p.name.startswith("kisti_flatten_")])

    def test_run_json_pipeline_overlap_partial_submit_falls_back_without_duplicate_loads(self):
        with tempfile.TemporaryDirectory() as td:
            from concurrent.futures import Future

            root = Path(td)
            (root / "x.jsonl").write_text(
                "\n".join(
                    json.dumps({"id": i, "x": f"v{i}", "items": [{"n": i}]})
                    for i in range(8)
                )
                + "\n",
                encoding="utf-8",
            )
            loaded_paths: list[Path] = []
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": True,
                "db_load_method": "auto",
                "parallel_workers": 2,
                "db_load_parallel_tables": 2,
                "overlap_batches": True,
                "tmp_dir": str(root),
                "persist_parquet_files": False,
                "persist_tsv_files": False,
            }

            class FakeCursor:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def execute(self, *_args, **_kwargs):
                    return None

                def fetchone(self):
                    return (1,)

            class FakeConnection:
                def cursor(self):
                    return FakeCursor()

                def commit(self):
                    return None

                def rollback(self):
                    return None

                def close(self):
                    return None

            class PartialSubmitExecutor:
                def __init__(self, *args, initializer=None, **_kwargs):
                    self.submit_count = 0
                    if initializer is not None:
                        initializer()

                def submit(self, fn, *args, **kwargs):
                    self.submit_count += 1
                    if self.submit_count == 2:
                        raise RuntimeError("submit failed")
                    fut = Future()
                    try:
                        fut.set_result(fn(*args, **kwargs))
                    except BaseException as exc:
                        fut.set_exception(exc)
                    return fut

                def shutdown(self, *args, **kwargs):
                    return None

            def fake_load_tsv(file_path, *args, **kwargs):
                loaded_paths.append(Path(file_path))
                return kwargs.get("name_map")

            with patch("concurrent.futures.ThreadPoolExecutor", PartialSubmitExecutor), patch(
                "pymysql.connect",
                return_value=FakeConnection(),
            ), patch(
                "KISTI_DB_Manager.manage.fill_table_from_tsv_file",
                side_effect=fake_load_tsv,
            ):
                res = run_json_pipeline(
                    data_config,
                    {"host": "h", "user": "u", "password": "p", "database": "d"},
                    chunk_size=8,
                    create=False,
                    load=True,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            self.assertEqual(
                {issue.stage for issue in res.report.issues},
                {"json_pipeline.load.overlap"},
            )
            self.assertGreaterEqual(len(loaded_paths), 2)
            self.assertEqual(len({str(p) for p in loaded_paths}), len(loaded_paths))
            self.assertFalse(list(root.glob("kisti_merge_*.tsv")))
            self.assertFalse(list(root.glob("kisti_union_*.tsv")))
            self.assertFalse([p for p in root.iterdir() if p.is_dir() and p.name.startswith("kisti_flatten_")])

    def test_run_json_pipeline_disables_unsafe_symlink_progress_path(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            external = root / "external"
            external.mkdir()
            progress_link = root / "progress_link"
            progress_link.symlink_to(external, target_is_directory=True)
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "progress_path": str(root / "missing_parent" / ".." / "progress_link" / "progress.json"),
                "progress_interval_s": 0,
            }

            res = run_json_pipeline(
                data_config,
                {"host": "h", "user": "u", "password": "p", "database": "d"},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertTrue(any(issue.stage == "json_pipeline.progress" for issue in res.report.issues))
            self.assertEqual(list(external.iterdir()), [])

    def test_run_json_pipeline_progress_write_rechecks_parent_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            progress_dir = root / "progress"
            progress_dir.mkdir()
            external = root / "external"
            external.mkdir()
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "progress_path": str(progress_dir / "progress.json"),
                "progress_interval_s": 0,
            }

            def fake_extract(batch_records, **_kwargs):
                progress_dir.rmdir()
                progress_dir.symlink_to(external, target_is_directory=True)
                return DummyDF(["id", "x"], rows=len(batch_records)), {}, {}

            res = run_json_pipeline(
                data_config,
                {"host": "h", "user": "u", "password": "p", "database": "d"},
                chunk_size=10,
                extract_fn=fake_extract,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertFalse(any(issue.stage == "json_pipeline.progress" for issue in res.report.issues))
            self.assertEqual(list(external.iterdir()), [])
            self.assertFalse((external / "progress.json").exists())

    def test_run_json_pipeline_progress_write_ignores_tmp_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            progress_dir = root / "progress"
            progress_dir.mkdir()
            external = root / "external_progress_tmp.json"
            external.write_text("keep", encoding="utf-8")
            (progress_dir / "progress.json.tmp").symlink_to(external)
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "progress_path": str(progress_dir / "progress.json"),
                "progress_interval_s": 0,
            }

            run_json_pipeline(
                data_config,
                {"host": "h", "user": "u", "password": "p", "database": "d"},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertEqual(external.read_text(encoding="utf-8"), "keep")
            self.assertTrue((progress_dir / "progress.json.tmp").is_symlink())
            self.assertTrue((progress_dir / "progress.json").exists())

    def test_run_json_pipeline_progress_replace_failure_does_not_unlink_external_tmp(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            progress_dir = root / "progress"
            backup_dir = root / "progress_backup"
            progress_dir.mkdir()
            external = root / "external"
            external.mkdir()
            progress_path = progress_dir / "progress.json"
            external_tmp: Path | None = None
            real_replace = os.replace

            def fake_replace(src, dst):
                nonlocal external_tmp
                dst_path = Path(dst)
                if dst_path == progress_path and external_tmp is None:
                    src_path = Path(src)
                    progress_dir.rename(backup_dir)
                    progress_dir.symlink_to(external, target_is_directory=True)
                    external_tmp = external / src_path.name
                    external_tmp.write_text("keep", encoding="utf-8")
                    raise RuntimeError("replace failed after parent swap")
                return real_replace(src, dst)

            with patch("os.replace", side_effect=fake_replace):
                res = run_json_pipeline(
                    {
                        "PATH": str(root),
                        "file_name": "x.jsonl",
                        "file_type": "jsonl",
                        "table_name": "base",
                        "KEY_SEP": "__",
                        "progress_path": str(progress_path),
                        "progress_interval_s": 0,
                        "persist_parquet_files": False,
                    },
                    {"host": "h", "user": "u", "password": "p", "database": "d"},
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=True,
                )

            self.assertFalse(res.report.issues)
            self.assertIsNotNone(external_tmp)
            self.assertEqual(external_tmp.read_text(encoding="utf-8"), "keep")
            self.assertFalse((external / "progress.json").exists())

    def test_run_json_pipeline_persists_parquet_artifacts_before_db_load(self):
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "parquet_out"
            data_config = {
                "PATH": str(Path(td)),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "json_streaming_load": False,
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def fake_iter_records(_dc, report=None, max_records=None, with_context=False):
                yield {"id": 1, "x": "a"}

            def fake_extract(batch_records, **_kwargs):
                return DummyDF(["id", "x"], rows=len(batch_records)), {"items": DummyDF(["id", "items__x"], rows=1)}, {}

            parquet_seen_during_load: list[bool] = []

            def fake_load(df, *_args, **_kwargs):
                parquet_seen_during_load.append(bool(list(out_dir.rglob("*.parquet"))))
                return _kwargs.get("name_map")

            with patch("KISTI_DB_Manager.pipeline._iter_json_records", side_effect=fake_iter_records), patch(
                "KISTI_DB_Manager.manage.fill_table_from_dataframe",
                side_effect=fake_load,
            ):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    extract_fn=fake_extract,
                    create=False,
                    load=True,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            persisted = sorted(out_dir.rglob("*.parquet"))
            self.assertEqual(len(persisted), 2)
            self.assertEqual((res.report.stats or {}).get("parquet_files_persisted"), 2)
            self.assertEqual((res.report.stats or {}).get("parquet_rows_emitted"), 2)
            self.assertTrue(parquet_seen_during_load)
            self.assertTrue(all(parquet_seen_during_load))

    def test_run_json_pipeline_auto_rust_arrow_falls_back_when_extension_missing(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "auto",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            from KISTI_DB_Manager.rust_arrow_backend import RustArrowBackendUnavailable

            with patch(
                "KISTI_DB_Manager.rust_arrow_backend._load_extension",
                side_effect=RustArrowBackendUnavailable("missing"),
            ):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            self.assertEqual(res.report.artifacts.get("flatten_backend"), "auto")
            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "python")
            self.assertIn("RustArrowBackendUnavailable", res.report.artifacts.get("flatten_backend_fallback_reason", ""))
            self.assertEqual(res.report.artifacts.get("rust_arrow_failed_batches"), 0)
            self.assertEqual(res.report.artifacts.get("python_fallback_active"), True)
            self.assertTrue(any(issue.stage == "json_pipeline.rust_arrow_auto_fallback" for issue in res.report.issues))
            self.assertTrue(list(out_dir.rglob("*.parquet")))

    def test_run_json_pipeline_auto_rust_arrow_falls_back_after_runtime_failure(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n{"id": 2, "x": "b"}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "auto",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}
            calls = []

            def fake_persist(_records, **kwargs):
                calls.append(int(kwargs["batch_idx"]))
                partial_dir = Path(kwargs["parquet_dir"], "base")
                partial_dir.mkdir(parents=True, exist_ok=True)
                (partial_dir / "b000000.parquet").write_text("partial", encoding="utf-8")
                raise RuntimeError("boom")

            with patch("KISTI_DB_Manager.rust_arrow_backend.persist_json_batch_to_parquet", side_effect=fake_persist):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=1,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            self.assertEqual(calls, [0])
            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "python")
            self.assertIn("RuntimeError: boom", res.report.artifacts.get("flatten_backend_fallback_reason", ""))
            self.assertEqual(res.report.artifacts.get("rust_arrow_failed_batches"), 1)
            self.assertEqual(res.report.artifacts.get("flatten_backend_fallback_batches"), 1)
            self.assertEqual(res.report.artifacts.get("python_fallback_active"), True)
            self.assertTrue(any(issue.stage == "json_pipeline.rust_arrow_auto_fallback" for issue in res.report.issues))
            base_files = sorted((out_dir / "base").glob("*.parquet"))
            self.assertEqual([p.name for p in base_files], ["b000000.parquet", "b000001.parquet"])

    def test_run_json_pipeline_auto_disables_rust_after_mixed_runtime_failure(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                '{"id": 1, "x": "a"}\n{"id": 2, "x": "b"}\n{"id": 3, "x": "c"}\n',
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "auto",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}
            calls = []

            def fake_persist(records, **kwargs):
                batch_idx = int(kwargs["batch_idx"])
                calls.append(batch_idx)
                table_dir = Path(kwargs["parquet_dir"], "base")
                table_dir.mkdir(parents=True, exist_ok=True)
                if batch_idx == 0:
                    (table_dir / "b000000.parquet").write_text("rust parquet", encoding="utf-8")
                    return {
                        "records_ok": len(records),
                        "records_failed": 0,
                        "parquet_files_persisted": 1,
                        "parquet_rows_emitted": len(records),
                        "parquet_tables_written": 1,
                        "tables": [{"table": "base", "columns": ["id", "x"], "rows": len(records)}],
                        "timings_ms": {"json.flatten": 1, "json.parquet.persist": 1},
                    }
                (table_dir / f"b{batch_idx:06d}.parquet").write_text("partial", encoding="utf-8")
                raise RuntimeError("boom")

            with patch("KISTI_DB_Manager.rust_arrow_backend.persist_json_batch_to_parquet", side_effect=fake_persist):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=1,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            self.assertEqual(calls, [0, 1])
            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "mixed")
            self.assertIn("RuntimeError: boom", res.report.artifacts.get("flatten_backend_fallback_reason", ""))
            self.assertEqual(res.report.artifacts.get("rust_arrow_failed_batches"), 1)
            self.assertEqual(res.report.artifacts.get("flatten_backend_fallback_batches"), 1)
            self.assertEqual(res.report.artifacts.get("python_fallback_active"), True)
            self.assertTrue(any(issue.stage == "json_pipeline.rust_arrow_auto_fallback" for issue in res.report.issues))

    def test_run_json_pipeline_explicit_rust_failure_skips_later_batches_and_cleans_partials(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n{"id": 2, "x": "b"}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
            }
            calls = []

            def fake_persist(records, **kwargs):
                batch_idx = int(kwargs["batch_idx"])
                calls.append(batch_idx)
                table_dir = Path(kwargs["parquet_dir"], "base")
                empty_dir = Path(kwargs["parquet_dir"], "empty_table")
                table_dir.mkdir(parents=True, exist_ok=True)
                empty_dir.mkdir(parents=True, exist_ok=True)
                (table_dir / f"b{batch_idx:06d}.parquet").write_text("partial", encoding="utf-8")
                raise RuntimeError("boom")

            with patch("KISTI_DB_Manager.rust_arrow_backend.persist_json_batch_to_parquet", side_effect=fake_persist):
                res = run_json_pipeline(
                    data_config,
                    {},
                    chunk_size=1,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=True,
                )

            self.assertEqual(calls, [0])
            self.assertTrue(any(issue.stage == "json_pipeline.rust_arrow" and issue.level == "error" for issue in res.report.issues))
            self.assertTrue(
                any("Skipping batch after earlier explicit Rust Arrow backend failure" in issue.message for issue in res.report.issues)
            )
            self.assertFalse(list(out_dir.rglob("*.parquet")))
            self.assertFalse((out_dir / "base").exists())
            self.assertFalse((out_dir / "empty_table").exists())

    def test_run_json_pipeline_auto_uses_python_when_excepted_expand_dict_enabled(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "a": {"x": 10}}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "except_keys": ["a"],
                "excepted_expand_dict": True,
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "auto",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=False,
            )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "python")
            self.assertFalse(res.report.issues)
            self.assertTrue(list((out_dir / "base__excepted__a").glob("*.parquet")))

    def test_run_json_pipeline_rust_arrow_rejects_excepted_expand_dict_enabled(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "a": {"x": 10}}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "except_keys": ["a"],
                "excepted_expand_dict": True,
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            issues = [issue.to_dict() for issue in res.report.issues]
            self.assertTrue(any("excepted_expand_dict=true" in str(issue.get("exception_message")) for issue in issues))
            self.assertFalse(list(out_dir.rglob("*.parquet")))

    def test_run_json_pipeline_auto_uses_rust_arrow_for_nested_parquet_values(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "items": ["bad", {"x": 1}]}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "auto",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            self.assertNotIn("flatten_backend_fallback_reason", res.report.artifacts)
            self.assertFalse(any(issue.stage == "json_pipeline.parquet_persist" for issue in res.report.issues))
            table = self._read_single_parquet_table(pq, out_dir, "base__items").to_pydict()
            self.assertEqual(table["id"], [1, 1])
            self.assertEqual(table["items"], ["bad", '{"x": 1}'])

    def test_run_json_pipeline_rust_arrow_raw_jsonl_parse_path(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"id": 1, "name": "ok"}),
                        "{bad json",
                        json.dumps(["not", "a", "dict"]),
                        json.dumps({"id": 2, "items": [{"x": 7}]}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            quarantine_path = root / "quarantine.jsonl"
            from KISTI_DB_Manager.quarantine import QuarantineWriter

            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
                "rust_raw_jsonl_parse": True,
            }

            res = run_json_pipeline(
                data_config,
                {},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
                quarantine=QuarantineWriter(quarantine_path),
            )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            self.assertEqual(res.report.artifacts.get("rust_raw_jsonl_parse_effective"), True)
            self.assertEqual(res.report.stats.get("records_read"), 4)
            self.assertEqual(res.report.stats.get("records_ok"), 2)
            self.assertEqual(res.report.stats.get("records_failed"), 2)
            main_table = self._read_single_parquet_table(pq, out_dir, "base").to_pydict()
            self.assertEqual(main_table["id"], [1, 2])
            sub_table = self._read_single_parquet_table(pq, out_dir, "base__items").to_pydict()
            self.assertEqual(sub_table["id"], [2])
            self.assertIn("bad json", quarantine_path.read_text(encoding="utf-8"))

    def test_run_json_pipeline_rust_arrow_direct_jsonl_file_parse_path(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"id": 1, "name": "ok"}),
                        "{bad json",
                        json.dumps(["not", "a", "dict"]),
                        json.dumps({"id": 2, "items": [{"x": 7}]}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            quarantine_path = root / "quarantine.jsonl"
            from KISTI_DB_Manager.quarantine import QuarantineWriter

            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
                "rust_raw_jsonl_parse": True,
                "rust_raw_jsonl_file_parse": True,
            }

            res = run_json_pipeline(
                data_config,
                {},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
                quarantine=QuarantineWriter(quarantine_path),
            )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            self.assertEqual(res.report.artifacts.get("rust_raw_jsonl_file_parse_effective"), True)
            self.assertEqual(res.report.stats.get("records_read"), 4)
            self.assertEqual(res.report.stats.get("records_ok"), 2)
            self.assertEqual(res.report.stats.get("records_failed"), 2)
            self.assertGreaterEqual(res.report.stats.get("parquet_batches_total", 0), 1)
            main_table = self._read_single_parquet_table(pq, out_dir, "base").to_pydict()
            self.assertEqual(main_table["id"], [1, 2])
            sub_table = self._read_single_parquet_table(pq, out_dir, "base__items").to_pydict()
            self.assertEqual(sub_table["id"], [2])
            quarantine_text = quarantine_path.read_text(encoding="utf-8")
            self.assertIn("bad json", quarantine_text)
            self.assertIn("non-dict JSON record", quarantine_text)

    def test_run_json_pipeline_rust_arrow_direct_jsonl_context_survives_failed_records(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "x.jsonl"
            source.write_text(
                "\n".join(
                    [
                        json.dumps({"id": 1, "name": "ok"}),
                        "{bad json",
                        json.dumps({"id": 2, "a": {"x": 10}}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            quarantine_path = root / "quarantine.jsonl"
            from KISTI_DB_Manager.quarantine import QuarantineWriter

            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "except_keys": ["a"],
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
                "rust_raw_jsonl_parse": True,
                "rust_raw_jsonl_file_parse": True,
            }

            res = run_json_pipeline(
                data_config,
                {},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
                quarantine=QuarantineWriter(quarantine_path),
            )

            self.assertEqual(res.report.stats.get("records_ok"), 2)
            self.assertEqual(res.report.stats.get("records_failed"), 1)
            excepted = self._read_single_parquet_table(pq, out_dir, "base__excepted__a").to_pydict()
            self.assertEqual(excepted["id"], [2])
            self.assertEqual(excepted["__line_no__"], [3])
            self.assertEqual(excepted["__record_index__"], [2])
            self.assertEqual(excepted["__source_path__"], [str(source)])

    def test_rust_arrow_raw_jsonl_rejects_integer_outside_u64(self):
        self._require_rust_arrow_with_pyarrow()

        from KISTI_DB_Manager.rust_arrow_backend import persist_json_lines_batch_to_parquet

        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "parquet_out"
            result = persist_json_lines_batch_to_parquet(
                ['{"id": 1, "big": 18446744073709551616}'],
                base_table="base",
                index_key="id",
                except_keys=[],
                excepted_expand_dict=False,
                sep="__",
                parquet_dir=out_dir,
                batch_idx=0,
                index_offset=0,
                record_contexts=[{"line_no": 1}],
                parallel_workers=0,
            )

            self.assertEqual(result["records_ok"], 0)
            self.assertEqual(result["records_failed"], 1)
            self.assertIn("outside supported i64/u64 range", result["errors"][0])
            self.assertFalse(list(out_dir.rglob("*.parquet")))

    def test_rust_arrow_raw_jsonl_rejects_nested_integer_with_path(self):
        self._require_rust_arrow_with_pyarrow()

        from KISTI_DB_Manager.rust_arrow_backend import persist_json_lines_batch_to_parquet

        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "parquet_out"
            result = persist_json_lines_batch_to_parquet(
                ['{"id": 1, "outer": {"items": [{"bad": 18446744073709551616}]}}'],
                base_table="base",
                index_key="id",
                except_keys=[],
                excepted_expand_dict=False,
                sep="__",
                parquet_dir=out_dir,
                batch_idx=0,
                index_offset=0,
                record_contexts=[{"line_no": 1}],
                parallel_workers=0,
            )

            self.assertEqual(result["records_ok"], 0)
            self.assertEqual(result["records_failed"], 1)
            self.assertIn("$.outer.items[0].bad", result["errors"][0])
            self.assertIn("outside supported i64/u64 range", result["errors"][0])
            self.assertFalse(list(out_dir.rglob("*.parquet")))

    def test_rust_arrow_raw_jsonl_simd_parser_matches_serde_contract(self):
        self._require_simd_json_parser()
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        from KISTI_DB_Manager.rust_arrow_backend import persist_json_lines_batch_to_parquet

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            results = {}
            for parser_backend in ("serde-json", "simd-json"):
                out_dir = root / parser_backend
                result = persist_json_lines_batch_to_parquet(
                    [
                        '{"id": 1, "name": "alpha", "items": [{"x": 7}]}',
                        '{"id": 2, "name": "beta", "items": [{"x": 8}]}',
                    ],
                    base_table="base",
                    index_key="id",
                    except_keys=[],
                    excepted_expand_dict=False,
                    sep="__",
                    parquet_dir=out_dir,
                    batch_idx=0,
                    index_offset=0,
                    record_contexts=[{"line_no": 1}, {"line_no": 2}],
                    parallel_workers=0,
                    parser_backend=parser_backend,
                )
                self.assertEqual(result["records_ok"], 2)
                self.assertEqual(result["parser_backend"], parser_backend)
                self.assertEqual(result["parser_fallbacks"], 0)
                results[parser_backend] = {
                    table_dir.name: self._read_single_parquet_table(pq, out_dir, table_dir.name).to_pydict()
                    for table_dir in sorted(out_dir.iterdir())
                    if table_dir.is_dir()
                }

            self.assertEqual(results["simd-json"], results["serde-json"])

    def test_rust_arrow_raw_jsonl_simd_parser_preserves_large_integer_validation(self):
        self._require_simd_json_parser()
        self._require_rust_arrow_with_pyarrow()

        from KISTI_DB_Manager.rust_arrow_backend import persist_json_lines_batch_to_parquet

        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "parquet_out"
            result = persist_json_lines_batch_to_parquet(
                ['{"id": 1, "outer": {"items": [{"bad": 18446744073709551616}]}}'],
                base_table="base",
                index_key="id",
                except_keys=[],
                excepted_expand_dict=False,
                sep="__",
                parquet_dir=out_dir,
                batch_idx=0,
                index_offset=0,
                record_contexts=[{"line_no": 1}],
                parallel_workers=0,
                parser_backend="simd-json",
            )

            self.assertEqual(result["records_ok"], 0)
            self.assertEqual(result["records_failed"], 1)
            self.assertEqual(result["parser_backend"], "simd-json")
            self.assertGreaterEqual(result["parser_fallbacks"], 1)
            self.assertIn("$.outer.items[0].bad", result["errors"][0])
            self.assertIn("outside supported i64/u64 range", result["errors"][0])
            self.assertFalse(list(out_dir.rglob("*.parquet")))

    def test_rust_arrow_direct_jsonl_rejects_excepted_nested_integer_during_flatten(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "x.jsonl"
            source.write_text(
                "\n".join(
                    [
                        '{"id": 1, "payload": {"nested": 18446744073709551616}}',
                        "",
                        '{"id": 2, "payload": {"nested": 7}}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            for columnar in (False, True):
                out_dir = root / ("parquet_columnar" if columnar else "parquet_rows")
                quarantine_path = root / ("quarantine_columnar.jsonl" if columnar else "quarantine_rows.jsonl")
                from KISTI_DB_Manager.quarantine import QuarantineWriter

                res = run_json_pipeline(
                    {
                        "PATH": str(root),
                        "file_name": "x.jsonl",
                        "file_type": "jsonl",
                        "table_name": "base",
                        "KEY_SEP": "__",
                        "except_keys": ["payload"],
                        "persist_parquet_files": True,
                        "persist_parquet_dir": str(out_dir),
                        "flatten_backend": "rust-arrow",
                        "rust_raw_jsonl_parse": True,
                        "rust_raw_jsonl_file_parse": True,
                        "rust_columnar_accumulator": columnar,
                    },
                    db_config,
                    chunk_size=1,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=True,
                    quarantine=QuarantineWriter(quarantine_path),
                )

                self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
                self.assertEqual(res.report.stats.get("records_read"), 2)
                self.assertEqual(res.report.stats.get("records_ok"), 1)
                self.assertEqual(res.report.stats.get("records_failed"), 1)
                quarantine_text = quarantine_path.read_text(encoding="utf-8")
                self.assertIn("$.payload.nested", quarantine_text)
                self.assertIn("outside supported i64/u64 range", quarantine_text)
                excepted = self._read_single_parquet_table(pq, out_dir, "base__excepted__payload").to_pydict()
                self.assertEqual(excepted["id"], [2])

    def test_run_json_pipeline_rust_arrow_cleans_partial_artifacts_on_id_compaction_failure(self):
        self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                (
                    '{"id": "https://openalex.org/W1", "items": ['
                    '{"author_id": "https://openalex.org/A1", "author_openalex_id": "A2"}'
                    "]}\n"
                ),
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
                "id_compaction": {"enabled": True},
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            issues = [issue.to_dict() for issue in res.report.issues]
            self.assertTrue(any(issue.get("stage") == "json_pipeline.rust_arrow" for issue in issues))
            self.assertFalse(list(out_dir.rglob("*.parquet")))

    def test_run_json_pipeline_rust_arrow_failure_does_not_delete_preexisting_parquet(self):
        self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                '{"id": "https://openalex.org/W1", "author_id": "https://openalex.org/A1", "author_openalex_id": "A2"}\n',
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            preexisting = out_dir / "base" / "b000000.parquet"
            preexisting.parent.mkdir(parents=True, exist_ok=True)
            preexisting.write_text("preexisting", encoding="utf-8")
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
                "id_compaction": {"enabled": True},
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertTrue(any(issue.stage == "json_pipeline.rust_arrow" for issue in res.report.issues))
            self.assertEqual(preexisting.read_text(encoding="utf-8"), "preexisting")
            self.assertEqual(sorted(p.relative_to(out_dir).as_posix() for p in out_dir.rglob("*.parquet")), ["base/b000000.parquet"])

    def test_run_json_pipeline_python_parquet_rejects_symlink_table_dir(self):
        self._require_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            external = root / "external"
            out_dir.mkdir()
            external.mkdir()
            (out_dir / "base").symlink_to(external, target_is_directory=True)
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "python",
            }

            res = run_json_pipeline(
                data_config,
                {},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertTrue(any(issue.stage == "json_pipeline.parquet_persist" for issue in res.report.issues))
            self.assertEqual(list(external.iterdir()), [])

    def test_run_json_pipeline_python_parquet_rejects_symlink_root_after_missing_parent_dotdot(self):
        self._require_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            external = root / "external"
            external.mkdir()
            out_link = root / "parquet_link"
            out_link.symlink_to(external, target_is_directory=True)
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(root / "missing_parent" / ".." / "parquet_link"),
                "flatten_backend": "python",
            }

            res = run_json_pipeline(
                data_config,
                {},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertTrue(any(issue.stage == "json_pipeline.parquet_persist" for issue in res.report.issues))
            self.assertEqual(list(external.iterdir()), [])

    def test_run_json_pipeline_python_parquet_skips_symlink_output_file(self):
        _pa, pq = self._require_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            table_dir = out_dir / "base"
            table_dir.mkdir(parents=True)
            target = root / "external.parquet"
            target.write_text("external", encoding="utf-8")
            (table_dir / "b000000.parquet").symlink_to(target)
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "python",
            }

            res = run_json_pipeline(
                data_config,
                {},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertFalse(res.report.issues)
            self.assertEqual(target.read_text(encoding="utf-8"), "external")
            self.assertTrue((table_dir / "b000000.parquet").is_symlink())
            table = pq.read_table(table_dir / "b000000_1.parquet").to_pydict()
            self.assertEqual(table["id"], [1])

    def test_run_json_pipeline_python_parquet_persists_mixed_scalar_list(self):
        _pa, pq = self._require_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                '{"id": "r1", "tags": ["a", "b"]}\n{"id": "r2", "tags": [3]}\n',
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "python",
            }

            res = run_json_pipeline(
                data_config,
                {},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertFalse(res.report.issues)
            table = self._read_single_parquet_table(pq, out_dir, "base__tags").to_pydict()
            self.assertEqual(table["id"], ["r1", "r1", "r2"])
            self.assertEqual(table["tags"], ["a", "b", "3"])

    def test_run_json_pipeline_rust_arrow_cleanup_skips_symlink_table_dir(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            external = root / "external"
            external.mkdir()
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def fake_persist(*args, **kwargs):
                table_link = Path(kwargs["parquet_dir"]) / "base"
                table_link.parent.mkdir(parents=True, exist_ok=True)
                table_link.symlink_to(external, target_is_directory=True)
                (external / "b000000.parquet").write_text("external", encoding="utf-8")
                raise RuntimeError("boom")

            with patch("KISTI_DB_Manager.rust_arrow_backend.persist_json_batch_to_parquet", side_effect=fake_persist):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=True,
                )

            self.assertTrue(any(issue.stage == "json_pipeline.rust_arrow" for issue in res.report.issues))
            self.assertEqual((external / "b000000.parquet").read_text(encoding="utf-8"), "external")

    def test_run_json_pipeline_rust_arrow_cleanup_skips_symlink_parquet_root(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            external = root / "external"
            external.mkdir()
            out_link = root / "parquet_link"
            out_link.symlink_to(external, target_is_directory=True)
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_link),
                "flatten_backend": "rust-arrow",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def fake_persist(*args, **kwargs):
                table_dir = Path(kwargs["parquet_dir"]) / "base"
                table_dir.mkdir(parents=True, exist_ok=True)
                (table_dir / "b000000.parquet").write_text("external", encoding="utf-8")
                raise RuntimeError("boom")

            with patch("KISTI_DB_Manager.rust_arrow_backend.persist_json_batch_to_parquet", side_effect=fake_persist):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=True,
                )

            self.assertTrue(any(issue.stage == "json_pipeline.rust_arrow" for issue in res.report.issues))
            self.assertEqual((external / "base" / "b000000.parquet").read_text(encoding="utf-8"), "external")

    def test_rust_arrow_backend_rejects_symlink_parquet_table_dir(self):
        self._require_rust_arrow_with_pyarrow()

        from KISTI_DB_Manager.rust_arrow_backend import persist_json_batch_to_parquet

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "parquet_out"
            out_dir.mkdir()
            external = root / "external"
            external.mkdir()
            (out_dir / "base").symlink_to(external, target_is_directory=True)

            with self.assertRaisesRegex(RuntimeError, "symlink"):
                persist_json_batch_to_parquet(
                    [{"id": 1, "x": "a"}],
                    base_table="base",
                    index_key="id",
                    except_keys=[],
                    excepted_expand_dict=False,
                    sep="__",
                    parquet_dir=out_dir,
                    batch_idx=0,
                    index_offset=0,
                    record_contexts=[],
                    parallel_workers=0,
                )

            self.assertEqual(list(external.iterdir()), [])

    def test_rust_arrow_backend_rejects_symlink_parquet_root(self):
        self._require_rust_arrow_with_pyarrow()

        from KISTI_DB_Manager.rust_arrow_backend import persist_json_batch_to_parquet

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            external = root / "external"
            external.mkdir()
            out_link = root / "parquet_link"
            out_link.symlink_to(external, target_is_directory=True)

            with self.assertRaisesRegex(RuntimeError, "symlink"):
                persist_json_batch_to_parquet(
                    [{"id": 1, "x": "a"}],
                    base_table="base",
                    index_key="id",
                    except_keys=[],
                    excepted_expand_dict=False,
                    sep="__",
                    parquet_dir=out_link,
                    batch_idx=0,
                    index_offset=0,
                    record_contexts=[],
                    parallel_workers=0,
                )

            self.assertEqual(list(external.iterdir()), [])

    def test_rust_arrow_backend_rejects_symlink_parquet_root_after_missing_parent_dotdot(self):
        self._require_rust_arrow_with_pyarrow()

        from KISTI_DB_Manager.rust_arrow_backend import persist_json_batch_to_parquet

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            external = root / "external"
            external.mkdir()
            out_link = root / "parquet_link"
            out_link.symlink_to(external, target_is_directory=True)

            with self.assertRaisesRegex(RuntimeError, "symlink"):
                persist_json_batch_to_parquet(
                    [{"id": 1, "x": "a"}],
                    base_table="base",
                    index_key="id",
                    except_keys=[],
                    excepted_expand_dict=False,
                    sep="__",
                    parquet_dir=root / "missing_parent" / ".." / "parquet_link",
                    batch_idx=0,
                    index_offset=0,
                    record_contexts=[],
                    parallel_workers=0,
                )

            self.assertEqual(list(external.iterdir()), [])

    def test_rust_arrow_backend_skips_dangling_symlink_parquet_path(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        from KISTI_DB_Manager.rust_arrow_backend import persist_json_batch_to_parquet

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "parquet_out"
            table_dir = out_dir / "base"
            table_dir.mkdir(parents=True)
            (table_dir / "b000000.parquet").symlink_to(root / "missing-target")

            result = persist_json_batch_to_parquet(
                [{"id": 1, "x": "a"}],
                base_table="base",
                index_key="id",
                except_keys=[],
                excepted_expand_dict=False,
                sep="__",
                parquet_dir=out_dir,
                batch_idx=0,
                index_offset=0,
                record_contexts=[],
                parallel_workers=0,
            )

            paths = sorted(p.name for p in table_dir.glob("*.parquet"))
            self.assertEqual(paths, ["b000000.parquet", "b000000_1.parquet"])
            self.assertFalse(list(table_dir.glob("*.tmp")))
            self.assertTrue((table_dir / "b000000.parquet").is_symlink())
            self.assertEqual(Path(result["tables"][0]["path"]).name, "b000000_1.parquet")
            data = pq.read_table(table_dir / "b000000_1.parquet").to_pydict()
            self.assertEqual(data["id"], [1])

    def test_run_json_pipeline_auto_does_not_write_lossy_float_for_python_int_outside_u64_range(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            huge_int = 18446744073709551616
            (root / "x.jsonl").write_text(f'{{"id": 1, "n": {huge_int}}}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "auto",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "python")
            self.assertIn("outside u64 range", res.report.artifacts.get("flatten_backend_fallback_reason", ""))
            self.assertFalse(any(issue.stage == "json_pipeline.parquet_persist" for issue in res.report.issues))
            table = self._read_single_parquet_table(pq, out_dir, "base").to_pydict()
            self.assertEqual(table["id"], [1])
            self.assertEqual(table["n"], [str(huge_int)])

    def test_run_json_pipeline_rust_arrow_preserves_u64_values_as_uint64(self):
        pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            value = 9223372036854775808
            (root / "x.jsonl").write_text(f'{{"id": 1, "n": {value}}}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=False,
            )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            table = self._read_single_parquet_table(pq, out_dir, "base")
            self.assertTrue(pa.types.is_string(table.schema.field("n").type) or pa.types.is_large_string(table.schema.field("n").type))
            self.assertEqual(table.to_pydict()["n"], [str(value)])

    def test_run_json_pipeline_rust_arrow_preserves_scalar_items_in_mixed_object_list(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "items": [{"a": 1}, "lost"]}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=False,
            )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            table = self._read_single_parquet_table(pq, out_dir, "base__items")
            data = table.to_pydict()
            self.assertEqual(data["id"], [1, 1])
            self.assertEqual(data["items__a"], [1, None])
            self.assertEqual(data["items"], [None, "lost"])

    def test_run_json_pipeline_uses_rust_arrow_direct_parquet_path(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n{"id": 2, "x": "b"}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def fake_persist(records, **kwargs):
                table_dir = Path(kwargs["parquet_dir"], "base")
                table_dir.mkdir(parents=True, exist_ok=True)
                path = table_dir / "b000000.parquet"
                path.write_text("fake", encoding="utf-8")
                return {
                    "records_ok": len(records),
                    "records_failed": 0,
                    "parquet_files_persisted": 1,
                    "parquet_rows_emitted": len(records),
                    "parquet_tables_written": 1,
                    "tables": [{"table": "base", "columns": ["id", "x"], "rows": len(records)}],
                    "timings_ms": {
                        "rust_arrow.read_line": 1,
                        "rust_arrow.py_to_json": 2,
                        "rust_arrow.number_validate": 3,
                        "json.flatten": 3,
                        "rust_arrow.columnar_merge": 4,
                        "json.parquet.persist": 4,
                        "rust_arrow.arrow_build": 5,
                        "rust_arrow.parquet_write": 6,
                        "rust_arrow.py_result_convert": 7,
                        "rust_arrow.total": 50,
                    },
                }

            with patch("KISTI_DB_Manager.rust_arrow_backend.persist_json_batch_to_parquet", side_effect=fake_persist):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            self.assertEqual(res.report.stats.get("records_ok"), 2)
            self.assertEqual(res.report.stats.get("parquet_files_persisted"), 1)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.read_line"), 1)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.py_to_json"), 2)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.number_validate"), 3)
            self.assertEqual(res.report.timings_ms.get("json.flatten"), 3)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.columnar_merge"), 4)
            self.assertEqual(res.report.timings_ms.get("json.parquet.persist"), 4)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.arrow_build"), 5)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.parquet_write"), 6)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.py_result_convert"), 7)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.total"), 50)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.unaccounted_ms"), 17)
            self.assertIn("base", res.name_maps)

    def test_run_json_pipeline_rust_arrow_feeds_existing_db_path_from_parquet(self):
        self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                '{"id": 1, "x": "a", "items": [{"n": 7}]}\n',
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
                "db_load_method": "to_sql",
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}
            parquet_seen_during_load: list[bool] = []
            loaded: dict[str, dict] = {}

            def fake_create(*_args, **kwargs):
                return kwargs.get("name_map")

            def fake_load(df, *_args, **kwargs):
                parquet_seen_during_load.append(bool(list(out_dir.rglob("*.parquet"))))
                loaded[str(kwargs.get("table_name"))] = df.to_dict(orient="list")
                return kwargs.get("name_map")

            with patch("KISTI_DB_Manager.manage.create_table_from_columns", side_effect=fake_create) as p_create, patch(
                "KISTI_DB_Manager.manage.fill_table_from_dataframe",
                side_effect=fake_load,
            ) as p_load:
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=True,
                    load=True,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            self.assertEqual(res.report.artifacts.get("rust_arrow_db_bridge"), "parquet_to_dataframe")
            self.assertEqual(p_create.call_count, 2)
            self.assertEqual(p_load.call_count, 2)
            self.assertTrue(parquet_seen_during_load)
            self.assertTrue(all(parquet_seen_during_load))
            self.assertEqual(loaded["base"]["id"], [1])
            self.assertEqual(loaded["base"]["x"], ["a"])
            self.assertEqual(loaded["base__items"]["id"], [1])
            self.assertEqual(loaded["base__items"]["items__n"], [7])
            self.assertEqual(res.report.stats.get("parquet_files_persisted"), 2)
            self.assertEqual(res.report.stats.get("rows_loaded"), 2)

    def test_run_json_pipeline_rust_arrow_can_use_direct_rust_db_loader(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text('{"id": 1, "x": "a"}\n', encoding="utf-8")
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
                "rust_db_load": True,
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def fake_persist(records, **kwargs):
                return {
                    "records_ok": len(records),
                    "records_failed": 0,
                    "parquet_files_persisted": 1,
                    "parquet_rows_emitted": len(records),
                    "parquet_tables_written": 1,
                    "tables": [
                        {
                            "table": "base",
                            "path": str(out_dir / "base" / "b000000.parquet"),
                            "columns": ["id", "x"],
                            "rows": len(records),
                        }
                    ],
                    "timings_ms": {
                        "rust_arrow.py_to_json": 2,
                        "json.flatten": 3,
                        "json.parquet.persist": 4,
                        "rust_arrow.total": 9,
                    },
                }

            def fake_create(*_args, **kwargs):
                return kwargs.get("name_map")

            with patch("KISTI_DB_Manager.rust_arrow_backend.persist_json_batch_to_parquet", side_effect=fake_persist), patch(
                "KISTI_DB_Manager.rust_arrow_backend.load_parquet_files_to_mysql",
                return_value={
                    "ok": True,
                    "files_loaded": 1,
                    "tables_loaded": 1,
                    "rows_loaded": 1,
                    "timings_ms": {"db.rust_mysql.load": 7},
                },
            ) as p_rust_load, patch("KISTI_DB_Manager.manage.create_table_from_columns", side_effect=fake_create), patch(
                "KISTI_DB_Manager.manage.fill_table_from_dataframe"
            ) as p_python_load:
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=True,
                    load=True,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            self.assertEqual(res.report.artifacts.get("rust_arrow_db_bridge"), "rust_mysql")
            self.assertEqual(res.report.artifacts.get("rust_db_load_effective"), True)
            self.assertEqual(res.report.stats.get("rows_loaded"), 1)
            self.assertEqual(res.report.stats.get("tables_loaded"), 1)
            self.assertEqual(res.report.stats.get("rust_db_load_ok"), 1)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.py_to_json"), 2)
            self.assertEqual(res.report.timings_ms.get("rust_arrow.total"), 9)
            self.assertEqual(res.report.timings_ms.get("db.rust_mysql.load"), 7)
            p_python_load.assert_not_called()
            args, kwargs = p_rust_load.call_args
            self.assertEqual(kwargs["db_config"]["database"], "d")
            self.assertEqual(args[0][0]["table_sql"], res.name_maps["base"].table_sql)
            self.assertEqual(args[0][0]["columns_original"], ["id", "x"])
            self.assertEqual(args[0][0]["columns_sql"], ["id", "x"])

    def test_run_json_pipeline_rust_arrow_preserves_scalar_parquet_types(self):
        pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                '{"id": 1, "n": 42, "b": true, "f": 1.5, "s": "x"}\n',
                encoding="utf-8",
            )
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def run_backend(backend: str):
                out_dir = root / f"parquet_{backend.replace('-', '_')}"
                data_config = {
                    "PATH": str(root),
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "base",
                    "KEY_SEP": "__",
                    "persist_parquet_files": True,
                    "persist_parquet_dir": str(out_dir),
                    "flatten_backend": backend,
                }
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )
                return res, self._read_single_parquet_table(pq, out_dir, "base")

            _py_res, py_table = run_backend("python")
            rust_res, rust_table = run_backend("rust-arrow")

            self.assertEqual(rust_res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            for col in ["id", "n", "b", "f"]:
                self.assertEqual(str(rust_table.schema.field(col).type), str(py_table.schema.field(col).type), col)
            self.assertTrue(pa.types.is_string(rust_table.schema.field("s").type) or pa.types.is_large_string(rust_table.schema.field("s").type))
            values = rust_table.to_pydict()
            self.assertEqual(values["id"], [1])
            self.assertEqual(values["n"], [42])
            self.assertEqual(values["b"], [True])
            self.assertEqual(values["f"], [1.5])
            self.assertNotIsInstance(values["id"][0], str)
            self.assertNotIsInstance(values["n"][0], str)
            self.assertNotIsInstance(values["b"][0], str)

    def test_run_json_pipeline_rust_arrow_columnar_accumulator_matches_row_accumulator(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "id": "r1",
                                "n": 1,
                                "b": True,
                                "f": 1.25,
                                "name": "alpha",
                                "items": [{"code": "A", "score": 10}, "loose"],
                                "tags": ["x", 2, None],
                                "meta": {"homepage_url": "https://example.org", "ids": {"openalex": "W1"}},
                                "extra": {"keep": 1},
                            }
                        ),
                        "{bad json",
                        json.dumps(
                            {
                                "id": "r2",
                                "n": 2,
                                "b": False,
                                "f": 2.5,
                                "name": None,
                                "items": [{"code": "B", "score": 11}, {"code": "C", "score": 12}],
                                "tags": [],
                                "meta": {"homepage_url": {"value": "intentional"}, "ids": {"doi": "10/abc"}},
                                "extra": [1, 2],
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def run_columnar(columnar: bool):
                out_dir = root / ("parquet_columnar" if columnar else "parquet_rows")
                res = run_json_pipeline(
                    {
                        "PATH": str(root),
                        "file_name": "x.jsonl",
                        "file_type": "jsonl",
                        "table_name": "base",
                        "KEY_SEP": "__",
                        "except_keys": ["extra"],
                        "persist_parquet_files": True,
                        "persist_parquet_dir": str(out_dir),
                        "flatten_backend": "rust-arrow",
                        "rust_raw_jsonl_parse": True,
                        "rust_raw_jsonl_file_parse": True,
                        "rust_columnar_accumulator": columnar,
                        "parallel_workers": 2,
                    },
                    db_config,
                    chunk_size=2,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=True,
                )
                tables = {
                    table_dir.name: self._read_single_parquet_table(pq, out_dir, table_dir.name)
                    for table_dir in sorted(out_dir.iterdir())
                    if table_dir.is_dir()
                }
                return res, tables

            row_res, row_tables = run_columnar(False)
            columnar_res, columnar_tables = run_columnar(True)

            self.assertEqual(row_res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            self.assertEqual(columnar_res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            self.assertEqual(columnar_res.report.artifacts.get("rust_columnar_accumulator"), True)
            self.assertEqual(columnar_res.report.artifacts.get("rust_raw_jsonl_file_parse_effective"), True)
            self.assertEqual(columnar_res.report.stats.get("records_read"), 3)
            self.assertEqual(columnar_res.report.stats.get("records_ok"), 2)
            self.assertEqual(columnar_res.report.stats.get("records_failed"), 1)
            self.assertEqual(sorted(columnar_tables), sorted(row_tables))
            for table_name in sorted(row_tables):
                row_table = row_tables[table_name]
                columnar_table = columnar_tables[table_name]
                self.assertEqual(columnar_table.to_pydict(), row_table.to_pydict(), table_name)
                self.assertEqual(
                    [(field.name, str(field.type)) for field in columnar_table.schema],
                    [(field.name, str(field.type)) for field in row_table.schema],
                    table_name,
                )

    def test_run_json_pipeline_rust_arrow_columnar_flush_records_decouples_output_batches(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                "\n".join(json.dumps({"id": i, "items": [{"x": i}], "extra": {"v": i}}) for i in range(1, 5)) + "\n",
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            res = run_json_pipeline(
                {
                    "PATH": str(root),
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "base",
                    "KEY_SEP": "__",
                    "except_keys": ["extra"],
                    "persist_parquet_files": True,
                    "persist_parquet_dir": str(out_dir),
                    "flatten_backend": "rust-arrow",
                    "rust_raw_jsonl_parse": True,
                    "rust_raw_jsonl_file_parse": True,
                    "rust_columnar_accumulator": True,
                    "rust_parquet_flush_records": 10,
                    "parallel_workers": 2,
                },
                {"host": "h", "user": "u", "password": "p", "database": "d"},
                chunk_size=1,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=False,
            )

            self.assertEqual(res.report.artifacts.get("rust_raw_jsonl_file_parse_effective"), True)
            self.assertEqual(res.report.artifacts.get("rust_columnar_accumulator"), True)
            self.assertEqual(res.report.artifacts.get("rust_parquet_flush_records"), 10)
            self.assertEqual(res.report.stats.get("records_ok"), 4)
            self.assertEqual(res.report.stats.get("parquet_batches_total"), 1)
            self.assertEqual(len(list((out_dir / "base").glob("*.parquet"))), 1)
            self.assertEqual(len(list((out_dir / "base__items").glob("*.parquet"))), 1)
            self.assertEqual(len(list((out_dir / "base__excepted__extra").glob("*.parquet"))), 1)
            base = self._read_single_parquet_table(pq, out_dir, "base").to_pydict()
            items = self._read_single_parquet_table(pq, out_dir, "base__items").to_pydict()
            excepted = self._read_single_parquet_table(pq, out_dir, "base__excepted__extra").to_pydict()
            self.assertEqual(base["id"], [1, 2, 3, 4])
            self.assertEqual(items["id"], [1, 2, 3, 4])
            self.assertEqual(items["items__x"], [1, 2, 3, 4])
            self.assertEqual(excepted["id"], [1, 2, 3, 4])
            self.assertEqual(excepted["__line_no__"], [1, 2, 3, 4])
            self.assertEqual(excepted["__record_index__"], [0, 1, 2, 3])
            self.assertEqual(excepted["value"], ['{"v":1}', '{"v":2}', '{"v":3}', '{"v":4}'])

    def test_run_json_pipeline_rust_arrow_matches_excepted_value_storage(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                '{"id": 1, "a": 7, "b": {"x": 10}, "c": [1, 2]}\n',
                encoding="utf-8",
            )
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def run_backend(backend: str):
                out_dir = root / f"parquet_excepted_{backend.replace('-', '_')}"
                data_config = {
                    "PATH": str(root),
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "base",
                    "KEY_SEP": "__",
                    "except_keys": ["a", "b", "c"],
                    "persist_parquet_files": True,
                    "persist_parquet_dir": str(out_dir),
                    "flatten_backend": backend,
                }
                run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )
                return out_dir

            py_out = run_backend("python")
            rust_out = run_backend("rust-arrow")

            for table_name, expected_value in [
                ("base__excepted__a", 7),
                ("base__excepted__b", '{"x":10}'),
                ("base__excepted__c", "[1,2]"),
            ]:
                py_values = self._read_single_parquet_table(pq, py_out, table_name).to_pydict()
                rust_values = self._read_single_parquet_table(pq, rust_out, table_name).to_pydict()
                self.assertEqual(rust_values["value"], py_values["value"], table_name)
                self.assertEqual(rust_values["value"], [expected_value], table_name)
                self.assertEqual(rust_values["__except_raw_json__"], py_values["__except_raw_json__"], table_name)
                self.assertEqual(rust_values["__except_path__"], py_values["__except_path__"], table_name)

    def test_run_json_pipeline_rust_arrow_writes_id_compaction_manifest(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                json.dumps(
                    {
                        "id": "https://openalex.org/W1",
                        "author_id": "https://openalex.org/A1",
                        "primary_location": {"source": {"id": "https://openalex.org/S1"}},
                        "referenced_works": ["https://openalex.org/W2"],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "rust-arrow",
                "id_compaction": {"enabled": True},
            }

            res = run_json_pipeline(
                data_config,
                {"host": "h", "user": "u", "password": "p", "database": "d"},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=False,
            )

            self.assertEqual(res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            main = self._read_single_parquet_table(pq, out_dir, "base").to_pydict()
            self.assertEqual(main["id"], ["W1"])
            self.assertEqual(main["author_openalex_id"], ["A1"])
            self.assertEqual(main["primary_location__source_openalex_id"], ["S1"])
            refs = self._read_single_parquet_table(pq, out_dir, "base__referenced_works").to_pydict()
            self.assertEqual(refs["id"], ["W1"])
            self.assertEqual(refs["referenced_work_openalex_id"], ["W2"])

            manifest = json.loads((out_dir / "schema_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["id_compaction"]["counts"]["base.author_openalex_id"], 1)
            self.assertEqual(
                manifest["id_compaction"]["counts"]["base__referenced_works.referenced_work_openalex_id"],
                1,
            )
            self.assertIn("author_openalex_id", manifest["tables"]["base"]["columns"])

    def test_run_json_pipeline_rust_arrow_matches_python_openalex_golden_contract(self):
        _pa, pq = self._require_rust_arrow_with_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                json.dumps(
                    {
                        "id": "https://openalex.org/W1",
                        "author_id": "https://openalex.org/A1",
                        "primary_location": {"source": {"id": "https://openalex.org/S1"}},
                        "referenced_works": ["https://openalex.org/W2"],
                        "authorships": [
                            {
                                "author": {"id": "https://openalex.org/A3"},
                                "institutions": [
                                    {
                                        "id": "https://openalex.org/I1",
                                        "ror": "https://ror.org/03yrm5c26",
                                    }
                                ],
                            }
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            def run_backend(backend: str):
                out_dir = root / backend.replace("-", "_")
                data_config = {
                    "PATH": str(root),
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "base",
                    "KEY_SEP": "__",
                    "persist_parquet_files": True,
                    "persist_parquet_dir": str(out_dir),
                    "flatten_backend": backend,
                    "id_compaction": {"enabled": True},
                }
                res = run_json_pipeline(
                    data_config,
                    {"host": "h", "user": "u", "password": "p", "database": "d"},
                    chunk_size=10,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )
                tables = {
                    table_dir.name: self._read_single_parquet_table(pq, out_dir, table_dir.name).to_pydict()
                    for table_dir in sorted(out_dir.iterdir())
                    if table_dir.is_dir()
                }
                manifest = json.loads((out_dir / "schema_manifest.json").read_text(encoding="utf-8"))
                return res, tables, manifest

            py_res, py_tables, py_manifest = run_backend("python")
            rust_res, rust_tables, rust_manifest = run_backend("rust-arrow")

            self.assertEqual(py_res.report.artifacts.get("flatten_backend_effective"), "python")
            self.assertEqual(rust_res.report.artifacts.get("flatten_backend_effective"), "rust-arrow")
            self.assertEqual(rust_tables, py_tables)
            self.assertEqual(rust_manifest["id_compaction"]["counts"], py_manifest["id_compaction"]["counts"])
            self.assertEqual(rust_manifest["tables"], py_manifest["tables"])

    def test_run_json_pipeline_writes_id_compaction_schema_manifest(self):
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "parquet_out"
            data_config = {
                "PATH": str(Path(td)),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "id_compaction": {"enabled": True},
            }
            db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

            def fake_iter_records(_dc, report=None, max_records=None, with_context=False):
                yield {"id": "https://openalex.org/W1", "author_id": "https://openalex.org/A1"}

            def fake_extract(batch_records, **kwargs):
                compactor = kwargs["id_compactor"]
                row = compactor.compact_row(batch_records[0], table_name="base")
                return DummyDF(list(row.keys()), rows=1), {}, {}

            with patch("KISTI_DB_Manager.pipeline._iter_json_records", side_effect=fake_iter_records):
                res = run_json_pipeline(
                    data_config,
                    db_config,
                    chunk_size=10,
                    extract_fn=fake_extract,
                    create=False,
                    load=False,
                    index=False,
                    optimize=False,
                    continue_on_error=False,
                )

            manifest_path = out_dir / "schema_manifest.json"
            self.assertTrue(manifest_path.exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["id_compaction"]["counts"]["base.author_openalex_id"], 1)
            self.assertIn("author_openalex_id", manifest["tables"]["base"]["columns"])
            self.assertEqual(res.report.artifacts["id_compaction"]["enabled"], True)

    def test_run_json_pipeline_schema_manifest_does_not_follow_symlink(self):
        _pa, pq = self._require_pyarrow()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "x.jsonl").write_text(
                '{"id": "https://openalex.org/W1", "author_id": "https://openalex.org/A1"}\n',
                encoding="utf-8",
            )
            out_dir = root / "parquet_out"
            out_dir.mkdir()
            external = root / "external_manifest.json"
            external.write_text("keep", encoding="utf-8")
            (out_dir / "schema_manifest.json").symlink_to(external)
            data_config = {
                "PATH": str(root),
                "file_name": "x.jsonl",
                "file_type": "jsonl",
                "table_name": "base",
                "KEY_SEP": "__",
                "persist_parquet_files": True,
                "persist_parquet_dir": str(out_dir),
                "flatten_backend": "python",
                "id_compaction": {"enabled": True},
            }

            res = run_json_pipeline(
                data_config,
                {},
                chunk_size=10,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

            self.assertTrue(any(issue.stage == "id_compaction.schema_manifest" for issue in res.report.issues))
            self.assertEqual(external.read_text(encoding="utf-8"), "keep")
            self.assertTrue((out_dir / "schema_manifest.json").is_symlink())
            table = self._read_single_parquet_table(pq, out_dir, "base").to_pydict()
            self.assertEqual(table["author_openalex_id"], ["A1"])


if __name__ == "__main__":
    unittest.main()
