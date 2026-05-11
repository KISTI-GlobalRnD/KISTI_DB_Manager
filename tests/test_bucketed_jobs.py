import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from KISTI_DB_Manager.bucketed_jobs import BucketedDuckDBJobSpec, BucketedPairSpec, run_bucketed_duckdb_job


class TestBucketedJobs(unittest.TestCase):
    def test_resume_skips_completed_source_batches_after_reduce_failure(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pyarrow is required for bucketed job smoke test: {exc}")

        with TemporaryDirectory() as td:
            root = Path(td)
            source_dir = root / "source"
            out_dir = root / "out"
            temp_dir = root / "temp"
            source_dir.mkdir()
            pq.write_table(pa.table({"id": [0, 1], "val": [1, 2]}), source_dir / "part-0.parquet")
            pq.write_table(pa.table({"id": [2, 3], "val": [3, 4]}), source_dir / "part-1.parquet")

            def build_batch_query(batch_files, bucket_count):
                paths = ", ".join(repr(str(path)) for path in batch_files)
                return (
                    "SELECT CAST(id % {bucket_count} AS INTEGER) AS bucket, id, val "
                    "FROM read_parquet([{paths}], union_by_name=true)"
                ).format(bucket_count=int(bucket_count), paths=paths)

            fail_once = {"enabled": True}

            def build_reduce_query(bucket_inputs):
                data_glob = bucket_inputs["data"]
                if data_glob and "bucket=1" in data_glob and fail_once["enabled"]:
                    fail_once["enabled"] = False
                    raise RuntimeError("forced reduce failure")
                if data_glob:
                    return (
                        "SELECT id, sum(val) AS val "
                        f"FROM read_parquet({data_glob!r}, union_by_name=true) "
                        "GROUP BY 1 ORDER BY 1"
                    )
                return "SELECT CAST(NULL AS BIGINT) AS id, CAST(NULL AS BIGINT) AS val WHERE FALSE"

            spec = BucketedDuckDBJobSpec(
                source_dir=source_dir,
                out_dir=out_dir,
                temp_dir=temp_dir,
                pair_specs=(BucketedPairSpec(name="data", build_batch_query=build_batch_query),),
                build_reduce_query=build_reduce_query,
                source_batch_files=1,
                bucket_count=2,
                cleanup_temp_on_success=False,
                resume=True,
            )

            with self.assertRaises(RuntimeError):
                run_bucketed_duckdb_job(spec)

            self.assertTrue((out_dir / "_bucketed_state" / "source_batches" / "batch-00000.json").exists())
            self.assertTrue((out_dir / "_bucketed_state" / "source_batches" / "batch-00001.json").exists())

            summary = run_bucketed_duckdb_job(spec)

            self.assertEqual(summary["status"], "done")
            self.assertEqual(summary["row_count"], 4)
            self.assertEqual(summary["bucket_count"], 2)


if __name__ == "__main__":
    unittest.main()
