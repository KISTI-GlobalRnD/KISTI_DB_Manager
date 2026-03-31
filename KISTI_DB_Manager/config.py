from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping


DEFAULT_FILE_SEP = "\t"
DEFAULT_KEY_SEP = "__"


def normalize_db_config(db_config: Mapping[str, Any]) -> dict[str, Any]:
    cfg = dict(db_config)
    cfg.setdefault("port", 3306)
    return cfg


def normalize_data_config(data_config: Mapping[str, Any]) -> dict[str, Any]:
    cfg = dict(data_config)

    # Backward compatible aliases
    if "FILE_SEP" in cfg and "SEP" not in cfg:
        cfg["SEP"] = cfg["FILE_SEP"]
    if "save_local_files" in cfg and "persist_parquet_files" not in cfg:
        cfg["persist_parquet_files"] = cfg["save_local_files"]
    if "save_local_dir" in cfg and "persist_parquet_dir" not in cfg:
        cfg["persist_parquet_dir"] = cfg["save_local_dir"]

    cfg.setdefault("SEP", DEFAULT_FILE_SEP)  # file delimiter
    cfg.setdefault("KEY_SEP", DEFAULT_KEY_SEP)  # nested key delimiter

    cfg.setdefault("Conv_DATETIME", False)
    cfg.setdefault("forced_null", False)
    cfg.setdefault("KEYs", [])
    cfg.setdefault("out_path", "")
    cfg.setdefault("fname_index", False)
    cfg.setdefault("chunksize", None)
    # DB load strategy (v2 performance)
    # - "auto": try fast bulk load (LOAD DATA LOCAL INFILE) then fall back to pandas.to_sql
    # - "load_data": force try fast load (still best-effort fallback by default)
    # - "to_sql": always use pandas.to_sql
    cfg.setdefault("db_load_method", "auto")
    # Robust ingestion knobs (v2)
    cfg.setdefault("include_extra_columns", True)
    cfg.setdefault("auto_alter_table", True)
    cfg.setdefault("fallback_on_insert_error", True)
    cfg.setdefault("fallback_column_type", "LONGTEXT")
    cfg.setdefault("insert_retry_max", 5)
    cfg.setdefault("index_prefix_len", 191)
    # JSON pipeline performance knobs
    cfg.setdefault("parallel_workers", 0)  # ProcessPool workers for JSON flatten (0/1 disables)
    cfg.setdefault("json_streaming_load", False)  # parquet-first/DataFrame path by default
    # Schema drift strategy (JSON-oriented)
    # - "evolve": add new columns (ALTER TABLE) when needed
    # - "freeze": do not ALTER; store unknown fields into extra_column_name (requires extra col)
    # - "hybrid": evolve for N warmup batches, then freeze (caps ALTER churn on huge inputs)
    cfg.setdefault("schema_mode", "evolve")
    cfg.setdefault("extra_column_name", "__extra__")
    cfg.setdefault("schema_hybrid_warmup_batches", 1)
    # Auto-except preflight (random sample -> high-cardinality dict path detection)
    cfg.setdefault("auto_except", False)
    cfg.setdefault("auto_except_sample_records", 5000)
    cfg.setdefault("auto_except_sample_max_sources", 64)
    cfg.setdefault("auto_except_seed", 42)
    cfg.setdefault("auto_except_unique_key_threshold", 512)
    cfg.setdefault("auto_except_min_observations", 20)
    cfg.setdefault("auto_except_novelty_threshold", 2.0)
    # excepted branch handling
    # - False (default): keep excepted payload in a single `value` field (+ raw JSON/context metadata)
    # - True: also expand dict keys as columns in excepted rows (legacy behavior; may cause column explosion)
    cfg.setdefault("excepted_expand_dict", False)
    # DB session tuning for ingest (best-effort; may require privileges)
    cfg.setdefault("fast_load_session", False)
    # Default JSON path: persist parquet locally before DB load.
    cfg.setdefault("persist_parquet_files", True)
    cfg.setdefault("persist_parquet_dir", "")
    # Optional local TSV artifacts from the streaming LOAD DATA backend.
    cfg.setdefault("persist_tsv_files", False)
    cfg.setdefault("persist_tsv_dir", "")

    return cfg


def join_path(path: str | Path, *parts: str) -> str:
    return str(Path(path).joinpath(*parts))


def coerce_db_config(db_config: Mapping[str, Any] | "DBConfig", *, inplace: bool = False) -> dict[str, Any]:
    """
    Accept DBConfig or mapping. When inplace=True and db_config is a mutable mapping,
    fill defaults into the same object (useful when callers expect mutations).
    """
    if isinstance(db_config, DBConfig):
        return normalize_db_config(db_config.to_dict())
    if inplace and isinstance(db_config, dict):
        db_config.update(normalize_db_config(db_config))
        return db_config
    return normalize_db_config(db_config)


def coerce_data_config(data_config: Mapping[str, Any] | "DataConfig", *, inplace: bool = False) -> dict[str, Any]:
    """
    Accept DataConfig or mapping. When inplace=True and data_config is a mutable mapping,
    fill defaults into the same object (useful when callers expect mutations).
    """
    if isinstance(data_config, DataConfig):
        return normalize_data_config(data_config.to_dict())
    if inplace and isinstance(data_config, dict):
        data_config.update(normalize_data_config(data_config))
        return data_config
    return normalize_data_config(data_config)


@dataclass(frozen=True)
class DBConfig:
    host: str
    user: str
    password: str
    database: str | None = None
    port: int = 3306

    @classmethod
    def from_mapping(cls, db_config: Mapping[str, Any]) -> "DBConfig":
        cfg = normalize_db_config(db_config)
        return cls(
            host=str(cfg["host"]),
            user=str(cfg["user"]),
            password=str(cfg["password"]),
            database=cfg.get("database"),
            port=int(cfg.get("port", 3306)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "port": self.port,
        }


@dataclass(frozen=True)
class DataConfig:
    PATH: str
    file_name: str
    table_name: str
    file_type: str

    # Delimiters
    SEP: str = DEFAULT_FILE_SEP
    KEY_SEP: str = DEFAULT_KEY_SEP

    # Optional knobs
    out_path: str = ""
    Conv_DATETIME: bool = False
    forced_null: bool = False
    KEY: str | None = None
    KEYs: list[str] = field(default_factory=list)
    fname_index: bool = False
    chunksize: int | None = None
    include_extra_columns: bool = True
    auto_alter_table: bool = True
    fallback_on_insert_error: bool = True
    fallback_column_type: str = "LONGTEXT"
    insert_retry_max: int = 5
    index_prefix_len: int = 191
    db_load_method: str = "auto"
    parallel_workers: int = 0
    json_streaming_load: bool = False
    schema_mode: str = "evolve"
    extra_column_name: str = "__extra__"
    schema_hybrid_warmup_batches: int = 1
    auto_except: bool = False
    auto_except_sample_records: int = 5000
    auto_except_sample_max_sources: int = 64
    auto_except_seed: int = 42
    auto_except_unique_key_threshold: int = 512
    auto_except_min_observations: int = 20
    auto_except_novelty_threshold: float = 2.0
    excepted_expand_dict: bool = False
    fast_load_session: bool = False
    persist_parquet_files: bool = True
    persist_parquet_dir: str = ""
    persist_tsv_files: bool = False
    persist_tsv_dir: str = ""

    @classmethod
    def from_mapping(cls, data_config: Mapping[str, Any]) -> "DataConfig":
        cfg = normalize_data_config(data_config)
        return cls(
            PATH=str(cfg["PATH"]),
            file_name=str(cfg["file_name"]),
            table_name=str(cfg["table_name"]),
            file_type=str(cfg["file_type"]),
            SEP=str(cfg.get("SEP", DEFAULT_FILE_SEP)),
            KEY_SEP=str(cfg.get("KEY_SEP", DEFAULT_KEY_SEP)),
            out_path=str(cfg.get("out_path", "")),
            Conv_DATETIME=bool(cfg.get("Conv_DATETIME", False)),
            forced_null=bool(cfg.get("forced_null", False)),
            KEY=cfg.get("KEY"),
            KEYs=list(cfg.get("KEYs", [])),
            fname_index=bool(cfg.get("fname_index", False)),
            chunksize=cfg.get("chunksize"),
            include_extra_columns=bool(cfg.get("include_extra_columns", True)),
            auto_alter_table=bool(cfg.get("auto_alter_table", True)),
            fallback_on_insert_error=bool(cfg.get("fallback_on_insert_error", True)),
            fallback_column_type=str(cfg.get("fallback_column_type", "LONGTEXT")),
            insert_retry_max=int(cfg.get("insert_retry_max", 5)),
            index_prefix_len=int(cfg.get("index_prefix_len", 191)),
            db_load_method=str(cfg.get("db_load_method", "auto")),
            parallel_workers=int(cfg.get("parallel_workers", 0) or 0),
            json_streaming_load=bool(cfg.get("json_streaming_load", False)),
            schema_mode=str(cfg.get("schema_mode", "evolve")),
            extra_column_name=str(cfg.get("extra_column_name", "__extra__")),
            schema_hybrid_warmup_batches=int(cfg.get("schema_hybrid_warmup_batches", 1) or 0),
            auto_except=bool(cfg.get("auto_except", False)),
            auto_except_sample_records=int(cfg.get("auto_except_sample_records", 5000) or 5000),
            auto_except_sample_max_sources=int(cfg.get("auto_except_sample_max_sources", 64) or 64),
            auto_except_seed=int(cfg.get("auto_except_seed", 42) or 42),
            auto_except_unique_key_threshold=int(cfg.get("auto_except_unique_key_threshold", 512) or 512),
            auto_except_min_observations=int(cfg.get("auto_except_min_observations", 20) or 20),
            auto_except_novelty_threshold=float(cfg.get("auto_except_novelty_threshold", 2.0) or 2.0),
            excepted_expand_dict=bool(cfg.get("excepted_expand_dict", False)),
            fast_load_session=bool(cfg.get("fast_load_session", False)),
            persist_parquet_files=bool(cfg.get("persist_parquet_files", True)),
            persist_parquet_dir=str(cfg.get("persist_parquet_dir", "")),
            persist_tsv_files=bool(cfg.get("persist_tsv_files", False)),
            persist_tsv_dir=str(cfg.get("persist_tsv_dir", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "PATH": self.PATH,
            "file_name": self.file_name,
            "table_name": self.table_name,
            "file_type": self.file_type,
            "SEP": self.SEP,
            "KEY_SEP": self.KEY_SEP,
            "out_path": self.out_path,
            "Conv_DATETIME": self.Conv_DATETIME,
            "forced_null": self.forced_null,
            "KEY": self.KEY,
            "KEYs": list(self.KEYs),
            "fname_index": self.fname_index,
            "chunksize": self.chunksize,
            "include_extra_columns": self.include_extra_columns,
            "auto_alter_table": self.auto_alter_table,
            "fallback_on_insert_error": self.fallback_on_insert_error,
            "fallback_column_type": self.fallback_column_type,
            "insert_retry_max": self.insert_retry_max,
            "index_prefix_len": self.index_prefix_len,
            "db_load_method": self.db_load_method,
            "parallel_workers": int(self.parallel_workers),
            "json_streaming_load": bool(self.json_streaming_load),
            "schema_mode": str(self.schema_mode),
            "extra_column_name": str(self.extra_column_name),
            "schema_hybrid_warmup_batches": int(self.schema_hybrid_warmup_batches),
            "auto_except": bool(self.auto_except),
            "auto_except_sample_records": int(self.auto_except_sample_records),
            "auto_except_sample_max_sources": int(self.auto_except_sample_max_sources),
            "auto_except_seed": int(self.auto_except_seed),
            "auto_except_unique_key_threshold": int(self.auto_except_unique_key_threshold),
            "auto_except_min_observations": int(self.auto_except_min_observations),
            "auto_except_novelty_threshold": float(self.auto_except_novelty_threshold),
            "excepted_expand_dict": bool(self.excepted_expand_dict),
            "fast_load_session": bool(self.fast_load_session),
            "persist_parquet_files": bool(self.persist_parquet_files),
            "persist_parquet_dir": str(self.persist_parquet_dir),
            "persist_tsv_files": bool(self.persist_tsv_files),
            "persist_tsv_dir": str(self.persist_tsv_dir),
        }
