# Chapter 6. Restart & Recovery

This chapter explains which checkpoints exist, what resume granularity is guaranteed, and what replay still happens after interruption.

## Parse stage

`parse-parquet*` runs leave progress in the run directory.
The important files are:

- `run_report.json.progress.json`
- optional external progress snapshots if you run external probes around the job

Operationally, parse resume is best understood as:

- source file or shard aware
- batch aware
- not row-level inside a batch

That means a restart usually replays at most the current batch rather than the full dataset.

## Materialize stage

`oa_materialize_parquet_to_db.py` stores progress at:

- `runs/<openalex_parse_run_dir>/parquet_materialize/progress.json`

The materializer now supports two resume granularities:

1. parquet file level by default
2. parquet file internal chunk level when `--file-chunk-rows N` is used

## Chunk-level resume

Example:

```bash
python scripts/oa_materialize_parquet_to_db.py \
  runs/<openalex_parse_run_dir> \
  --dotenv path/to/.env \
  --file-chunk-rows 5000
```

With this enabled, `progress.json` stores partial progress like:

- `partial_files.<table>.<file>.next_offset`
- `partial_files.<table>.<file>.chunk_rows`
- `partial_files.<table>.<file>.total_rows`

On restart, the loader continues from `next_offset` instead of replaying the whole parquet file.

## Practical guidance

- For small parquet files, chunk resume adds little value
- For large OpenAlex subtables, chunk resume is worth using
- Keep `file_chunk_rows` moderate so checkpoint frequency is useful without adding too much overhead
