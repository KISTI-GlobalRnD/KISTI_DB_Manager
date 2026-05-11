from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

from .runstate import JsonRunState, atomic_write_json, read_json, utc_now_iso


@dataclass(frozen=True)
class BucketedPairSpec:
    name: str
    build_batch_query: Callable[[Sequence[Path], int], str]


@dataclass(frozen=True)
class BucketedDuckDBJobSpec:
    source_dir: Path
    out_dir: Path
    pair_specs: tuple[BucketedPairSpec, ...]
    build_reduce_query: Callable[[dict[str, str | None]], str]
    temp_dir: Path | None = None
    threads: int = 4
    memory_limit: str = "48GB"
    max_rows_per_file: int = 1_000_000
    source_batch_files: int = 8
    bucket_count: int = 256
    cleanup_temp_on_success: bool = True
    resume: bool = True


def run_bucketed_duckdb_job(spec: BucketedDuckDBJobSpec) -> dict[str, Any]:
    import duckdb
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
    import traceback

    source_dir = Path(spec.source_dir).expanduser().resolve()
    out_dir = Path(spec.out_dir).expanduser().resolve()
    temp_dir = Path(spec.temp_dir).expanduser().resolve() if spec.temp_dir else None
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "build.log"
    progress_path = out_dir / "progress.json"
    summary_path = out_dir / "summary.json"
    state_dir = out_dir / "_bucketed_state"
    batch_done_dir = state_dir / "source_batches"
    bucket_done_dir = state_dir / "reduce_buckets"
    source_files = sorted(source_dir.glob("*.parquet"))
    if not source_files:
        raise FileNotFoundError(f"no parquet files found under {source_dir}")
    batch_size = max(1, int(spec.source_batch_files))
    bucket_count = max(1, int(spec.bucket_count))
    total_batches = -(-len(source_files) // batch_size)
    if spec.resume and summary_path.exists():
        try:
            summary = read_json(summary_path)
        except Exception:
            summary = {}
        if (
            str(summary.get("status") or "") == "done"
            and str(summary.get("source_dir") or "") == str(source_dir)
            and int(summary.get("source_batch_files") or 0) == int(batch_size)
            and int(summary.get("bucket_count") or 0) == int(bucket_count)
            and int(summary.get("source_parquet_files_total") or 0) == int(len(source_files))
            and list(summary.get("pair_specs") or []) == [item.name for item in spec.pair_specs]
        ):
            return summary
    if not spec.resume:
        log_path.unlink(missing_ok=True)
        summary_path.unlink(missing_ok=True)
        shutil.rmtree(state_dir, ignore_errors=True)
        for parquet_file in out_dir.glob("*.parquet"):
            parquet_file.unlink(missing_ok=True)

    if temp_dir:
        if not spec.resume:
            shutil.rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        duckdb_temp_dir = temp_dir / "duckdb_tmp"
        pair_root = temp_dir / "pair_buckets"
    else:
        duckdb_temp_dir = out_dir / "_duckdb_tmp"
        pair_root = out_dir / "_pair_buckets"
        if not spec.resume:
            shutil.rmtree(duckdb_temp_dir, ignore_errors=True)
            shutil.rmtree(pair_root, ignore_errors=True)
    duckdb_temp_dir.mkdir(parents=True, exist_ok=True)
    pair_root.mkdir(parents=True, exist_ok=True)
    batch_done_dir.mkdir(parents=True, exist_ok=True)
    bucket_done_dir.mkdir(parents=True, exist_ok=True)

    pair_dirs = {item.name: pair_root / item.name for item in spec.pair_specs}
    for pair_dir in pair_dirs.values():
        pair_dir.mkdir(parents=True, exist_ok=True)

    partitioning = ds.partitioning(pa.schema([("bucket", pa.int32())]), flavor="hive")

    def log(message: str) -> None:
        from datetime import datetime

        with log_path.open("a", encoding="utf-8") as fp:
            fp.write(f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] {message}\n")

    def batch_marker_path(batch_index: int) -> Path:
        return batch_done_dir / f"batch-{batch_index:05d}.json"

    def bucket_marker_path(bucket_index: int) -> Path:
        return bucket_done_dir / f"bucket-{bucket_index:04d}.json"

    def marker_done(path: Path) -> bool:
        if not spec.resume or not path.exists() or path.stat().st_size <= 0:
            return False
        try:
            payload = read_json(path)
        except Exception:
            return False
        return str(payload.get("status") or "") == "done"

    def count_done_markers(root: Path) -> int:
        return sum(1 for path in root.glob("*.json") if marker_done(path))

    def cleanup_batch_outputs(batch_index: int) -> None:
        for item in spec.pair_specs:
            for bucket_dir in pair_dirs[item.name].glob("bucket=*"):
                for parquet_file in bucket_dir.glob(f"{item.name}-batch-{batch_index:05d}-*.parquet"):
                    parquet_file.unlink(missing_ok=True)

    def cleanup_bucket_outputs(bucket_index: int) -> None:
        for parquet_file in out_dir.glob(f"part-bucket-{bucket_index:04d}-*.parquet"):
            parquet_file.unlink(missing_ok=True)

    progress_state = JsonRunState.create(
        progress_path,
        {
            "status": "running",
            "generated_at": utc_now_iso(),
            "source_dir": str(source_dir),
            "out_dir": str(out_dir),
            "threads": int(spec.threads),
            "memory_limit": str(spec.memory_limit),
            "source_batch_files": int(batch_size),
            "bucket_count": int(bucket_count),
            "source_parquet_files_total": int(len(source_files)),
            "source_batches_total": int(total_batches),
            "source_batches_done": count_done_markers(batch_done_dir),
            "buckets_total": int(bucket_count),
            "buckets_done": count_done_markers(bucket_done_dir),
            "phase": "bucket_pairs",
            "pair_root": str(pair_root),
            "pair_specs": [item.name for item in spec.pair_specs],
            "resume": bool(spec.resume),
        },
    )

    def update_progress(**changes: Any) -> None:
        progress_state.update(**changes)

    def connect() -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(database=":memory:")
        con.execute(f"PRAGMA threads={max(1, int(spec.threads))}")
        con.execute(f"SET memory_limit={json.dumps(str(spec.memory_limit))}")
        con.execute("SET preserve_insertion_order=false")
        con.execute(f"SET temp_directory={json.dumps(str(duckdb_temp_dir))}")
        return con

    def write_pair_dataset(*, query: str, base_dir: Path, basename_prefix: str, batch_index: int) -> None:
        con = connect()
        try:
            reader = con.execute(query).to_arrow_reader()
            ds.write_dataset(
                reader,
                base_dir=str(base_dir),
                format="parquet",
                partitioning=partitioning,
                existing_data_behavior="overwrite_or_ignore",
                basename_template=f"{basename_prefix}-batch-{batch_index:05d}-{{i}}.parquet",
                max_rows_per_file=max(1, int(spec.max_rows_per_file)),
                max_rows_per_group=min(max(1, int(spec.max_rows_per_file)), 100_000),
            )
        finally:
            con.close()

    def aggregate_bucket_output(*, bucket_index: int) -> None:
        bucket_inputs: dict[str, str | None] = {}
        any_present = False
        for item in spec.pair_specs:
            glob = pair_dirs[item.name] / f"bucket={bucket_index}" / "*.parquet"
            exists = any(glob.parent.glob("*.parquet"))
            if exists:
                any_present = True
                bucket_inputs[item.name] = str(glob)
            else:
                bucket_inputs[item.name] = None
        if not any_present:
            return
        final_sql = spec.build_reduce_query(bucket_inputs)
        con = connect()
        try:
            reader = con.execute(final_sql).to_arrow_reader()
            ds.write_dataset(
                reader,
                base_dir=str(out_dir),
                format="parquet",
                existing_data_behavior="overwrite_or_ignore",
                basename_template=f"part-bucket-{bucket_index:04d}-{{i}}.parquet",
                max_rows_per_file=max(1, int(spec.max_rows_per_file)),
                max_rows_per_group=min(max(1, int(spec.max_rows_per_file)), 100_000),
            )
        finally:
            con.close()

    log(f"source_dir={source_dir}")
    log(f"temp_dir={temp_dir or duckdb_temp_dir.parent}")
    log(f"duckdb_temp_dir={duckdb_temp_dir}")
    log(f"pair_root={pair_root}")
    log(f"threads={int(spec.threads)}")
    log(f"memory_limit={spec.memory_limit}")
    try:
        for batch_index in range(total_batches):
            start = batch_index * batch_size
            batch_files = source_files[start : start + batch_size]
            marker_path = batch_marker_path(batch_index)
            if marker_done(marker_path):
                update_progress(
                    source_batches_done=count_done_markers(batch_done_dir),
                    phase="bucket_pairs",
                    current_batch=batch_index + 1,
                )
                log(f"bucket_pairs_skip_done batch={batch_index + 1}/{total_batches}")
                continue
            cleanup_batch_outputs(batch_index)
            log(
                "bucket_pairs_start "
                f"batch={batch_index + 1}/{total_batches} "
                f"files={len(batch_files)} "
                f"first={batch_files[0].name} "
                f"last={batch_files[-1].name}"
            )
            for item in spec.pair_specs:
                query = item.build_batch_query(batch_files, bucket_count)
                write_pair_dataset(
                    query=query,
                    base_dir=pair_dirs[item.name],
                    basename_prefix=item.name,
                    batch_index=batch_index,
                )
            atomic_write_json(
                marker_path,
                {
                    "status": "done",
                    "generated_at": utc_now_iso(),
                    "batch_index": int(batch_index),
                    "source_files": [str(path) for path in batch_files],
                    "pair_specs": [item.name for item in spec.pair_specs],
                },
            )
            update_progress(
                source_batches_done=count_done_markers(batch_done_dir),
                phase="bucket_pairs",
                current_batch=batch_index + 1,
            )
            log(f"bucket_pairs_done batch={batch_index + 1}/{total_batches}")

        update_progress(phase="aggregate_buckets", current_batch=None)
        for bucket_index in range(bucket_count):
            marker_path = bucket_marker_path(bucket_index)
            if marker_done(marker_path):
                update_progress(
                    buckets_done=count_done_markers(bucket_done_dir),
                    phase="aggregate_buckets",
                    current_bucket=bucket_index + 1,
                )
                log(f"aggregate_bucket_skip_done bucket={bucket_index + 1}/{bucket_count}")
                continue
            cleanup_bucket_outputs(bucket_index)
            log(f"aggregate_bucket_start bucket={bucket_index + 1}/{bucket_count}")
            aggregate_bucket_output(bucket_index=bucket_index)
            atomic_write_json(
                marker_path,
                {
                    "status": "done",
                    "generated_at": utc_now_iso(),
                    "bucket_index": int(bucket_index),
                },
            )
            update_progress(
                buckets_done=count_done_markers(bucket_done_dir),
                phase="aggregate_buckets",
                current_bucket=bucket_index + 1,
            )
            log(f"aggregate_bucket_done bucket={bucket_index + 1}/{bucket_count}")
    except Exception as exc:
        tb = traceback.format_exc()
        log(f"error={type(exc).__name__}: {exc}")
        log(tb.rstrip())
        failure = {
            "status": "failed",
            "generated_at": utc_now_iso(),
            "source_dir": str(source_dir),
            "out_dir": str(out_dir),
            "threads": int(spec.threads),
            "memory_limit": str(spec.memory_limit),
            "source_batch_files": int(batch_size),
            "bucket_count": int(bucket_count),
            "phase": progress_state.payload.get("phase"),
            "source_batches_done": int(progress_state.payload.get("source_batches_done", 0)),
            "buckets_done": int(progress_state.payload.get("buckets_done", 0)),
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        progress_state.payload = dict(failure)
        progress_state.write(touch_timestamp=False)
        atomic_summary = JsonRunState.create(summary_path, failure, write=False)
        atomic_summary.write(touch_timestamp=False)
        raise

    parquet_files = sorted(out_dir.glob("*.parquet"))
    row_count = 0
    for parquet_file in parquet_files:
        row_count += int(pq.ParquetFile(parquet_file).metadata.num_rows)

    summary = {
        "status": "done",
        "generated_at": utc_now_iso(),
        "source_dir": str(source_dir),
        "out_dir": str(out_dir),
        "row_count": int(row_count),
        "parquet_files": int(len(parquet_files)),
        "source_parquet_files_total": int(len(source_files)),
        "source_batches_total": int(total_batches),
        "source_batch_files": int(batch_size),
        "bucket_count": int(bucket_count),
        "pair_specs": [item.name for item in spec.pair_specs],
    }
    progress_state.payload = dict(summary)
    progress_state.write(touch_timestamp=False)
    summary_state = JsonRunState.create(summary_path, summary, write=False)
    summary_state.write(touch_timestamp=False)
    log(f"done rows={row_count} files={len(parquet_files)}")
    if spec.cleanup_temp_on_success:
        shutil.rmtree(pair_root, ignore_errors=True)
        shutil.rmtree(duckdb_temp_dir, ignore_errors=True)
    return summary
