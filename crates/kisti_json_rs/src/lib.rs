#![allow(clippy::useless_conversion)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, BooleanBuilder, Float32Array, Float64Array,
    Float64Builder, Int16Array, Int32Array, Int64Array, Int64Builder, Int8Array, LargeBinaryArray,
    LargeStringArray, LargeStringBuilder, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use mysql::prelude::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyList, PyTuple};
use rayon::prelude::*;
use serde_json::{Number, Value};

type Row = BTreeMap<String, Value>;
type TableRows = BTreeMap<String, Vec<Row>>;
type IndexedJsonValues = Vec<(usize, Value)>;

static RAYON_POOLS: OnceLock<Mutex<HashMap<usize, Arc<rayon::ThreadPool>>>> = OnceLock::new();

#[derive(Clone)]
struct IdCompactionOptions {
    enabled: bool,
    preset: String,
    mode: String,
    description_policy: String,
    apply_to_excepted_raw_json: bool,
    collision_policy: String,
    namespace_conflict_policy: String,
    rules_version: String,
    rules_hash: String,
}

impl Default for IdCompactionOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            preset: "openalex".to_string(),
            mode: "semantic_column_strip".to_string(),
            description_policy: "required".to_string(),
            apply_to_excepted_raw_json: false,
            collision_policy: "error".to_string(),
            namespace_conflict_policy: "error".to_string(),
            rules_version: "openalex-semantic-column-strip-v2".to_string(),
            rules_hash: String::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct IdColumnMeta {
    table: String,
    original_column: String,
    new_column: String,
    namespace: String,
    entity: Option<String>,
    removed_prefix: String,
    description: String,
    count: usize,
}

#[derive(Default)]
struct IdCompactionState {
    columns: BTreeMap<String, IdColumnMeta>,
    counts: BTreeMap<String, usize>,
    ambiguous_counts: BTreeMap<String, usize>,
    collision_counts: BTreeMap<String, usize>,
    namespace_conflict_counts: BTreeMap<String, usize>,
}

#[derive(Clone)]
struct Options {
    base_table: String,
    index_key: String,
    except_keys: BTreeSet<String>,
    excepted_expand_dict: bool,
    sep: String,
    parquet_dir: PathBuf,
    batch_idx: usize,
    index_offset: usize,
    parallel_workers: usize,
    parallel_table_writes: bool,
    columnar_accumulator: bool,
    record_contexts: Vec<Value>,
    id_compaction: IdCompactionOptions,
}

#[derive(Clone)]
struct RecordOut {
    ok: bool,
    main: Row,
    subs: TableRows,
    excepted: TableRows,
    error: Option<String>,
}

struct PersistExtras {
    initial_records_failed: usize,
    errors: Vec<String>,
    error_indices: Vec<usize>,
    timings_ms: Vec<(&'static str, u64)>,
}

struct TableMeta {
    table: String,
    path: String,
    columns: Vec<String>,
    rows: usize,
}

struct PersistOutput {
    records_read: usize,
    bytes_read: usize,
    records_ok: usize,
    records_failed: usize,
    parquet_files_persisted: usize,
    parquet_rows_emitted: usize,
    parquet_batches_total: usize,
    tables: Vec<TableMeta>,
    errors: Vec<String>,
    error_indices: Vec<usize>,
    error_records: Vec<JsonlErrorRecord>,
    timings_ms: Vec<(&'static str, u64)>,
    id_compaction_state: Option<IdCompactionState>,
}

struct RustMysqlTable {
    path: PathBuf,
    table_sql: String,
    columns_original: Vec<String>,
    columns_sql: Vec<String>,
}

struct RustMysqlTableMeta {
    table_sql: String,
    path: String,
    rows_loaded: usize,
}

struct JsonlErrorRecord {
    source_path: String,
    line_no: usize,
    record_index: usize,
    raw_line: String,
    error: String,
}

fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(v) = obj.extract::<bool>() {
        return Ok(Value::Bool(v));
    }
    if let Ok(v) = obj.extract::<i64>() {
        return Ok(Value::Number(Number::from(v)));
    }
    if let Ok(v) = obj.extract::<u64>() {
        return Ok(Value::Number(Number::from(v)));
    }
    if !obj.is_instance_of::<PyFloat>()
        && obj.extract::<String>().is_err()
        && obj.downcast::<PyDict>().is_err()
        && obj.downcast::<PyList>().is_err()
        && obj.downcast::<PyTuple>().is_err()
    {
        return Err(PyRuntimeError::new_err(
            "rust-arrow unsupported Python value or integer outside u64 range; use the Python backend",
        ));
    }
    if let Ok(v) = obj.extract::<f64>() {
        if let Some(n) = Number::from_f64(v) {
            return Ok(Value::Number(n));
        }
        return Ok(Value::Null);
    }
    if let Ok(v) = obj.extract::<String>() {
        return Ok(Value::String(v));
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key = k.str()?.to_string();
            map.insert(key, py_to_json(&v)?);
        }
        return Ok(Value::Object(map));
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut values = Vec::with_capacity(list.len());
        for item in list.iter() {
            values.push(py_to_json(&item)?);
        }
        return Ok(Value::Array(values));
    }
    if let Ok(tuple) = obj.downcast::<PyTuple>() {
        let mut values = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            values.push(py_to_json(&item)?);
        }
        return Ok(Value::Array(values));
    }
    Ok(Value::String(obj.str()?.to_string()))
}

fn parse_json_line_value(obj: &Bound<'_, PyAny>) -> PyResult<Result<Option<Value>, String>> {
    if let Ok(v) = obj.downcast::<PyBytes>() {
        let bytes = v.as_bytes();
        return Ok(parse_json_bytes_value(bytes));
    }
    if let Ok(v) = obj.extract::<String>() {
        return Ok(parse_json_bytes_value(v.as_bytes()));
    }
    if let Ok(v) = obj.extract::<Vec<u8>>() {
        return Ok(parse_json_bytes_value(&v));
    }
    Err(PyRuntimeError::new_err(
        "rust-arrow JSONL input records must be str or bytes",
    ))
}

fn parse_json_bytes_value(bytes: &[u8]) -> Result<Option<Value>, String> {
    let trimmed = trim_ascii_whitespace(bytes);
    if trimmed.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice::<Value>(trimmed)
        .map(Some)
        .map_err(|e| format!("failed to parse JSONL line: {e}"))
}

fn trim_ascii_whitespace(mut bytes: &[u8]) -> &[u8] {
    while let Some((first, rest)) = bytes.split_first() {
        if first.is_ascii_whitespace() {
            bytes = rest;
        } else {
            break;
        }
    }
    while let Some((last, rest)) = bytes.split_last() {
        if last.is_ascii_whitespace() {
            bytes = rest;
        } else {
            break;
        }
    }
    bytes
}

fn get_string_option(options: &Bound<'_, PyDict>, key: &str, default: &str) -> PyResult<String> {
    match options.get_item(key)? {
        Some(value) => Ok(value.extract::<String>()?),
        None => Ok(default.to_string()),
    }
}

fn rayon_pool(workers: usize) -> PyResult<Arc<rayon::ThreadPool>> {
    let pools = RAYON_POOLS.get_or_init(|| Mutex::new(HashMap::new()));
    {
        let guard = pools.lock().map_err(|e| {
            PyRuntimeError::new_err(format!("rust-arrow rayon pool cache poisoned: {e}"))
        })?;
        if let Some(pool) = guard.get(&workers) {
            return Ok(pool.clone());
        }
    }
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
    );
    let mut guard = pools.lock().map_err(|e| {
        PyRuntimeError::new_err(format!("rust-arrow rayon pool cache poisoned: {e}"))
    })?;
    if let Some(existing) = guard.get(&workers) {
        return Ok(existing.clone());
    }
    guard.insert(workers, pool.clone());
    Ok(pool)
}

fn get_bool_option(options: &Bound<'_, PyDict>, key: &str, default: bool) -> PyResult<bool> {
    match options.get_item(key)? {
        Some(value) => Ok(value.extract::<bool>()?),
        None => Ok(default),
    }
}

fn get_usize_option(options: &Bound<'_, PyDict>, key: &str, default: usize) -> PyResult<usize> {
    match options.get_item(key)? {
        Some(value) => Ok(value.extract::<usize>()?),
        None => Ok(default),
    }
}

fn parse_id_compaction_options(options: &Bound<'_, PyDict>) -> PyResult<IdCompactionOptions> {
    let Some(value) = options.get_item("id_compaction")? else {
        return Ok(IdCompactionOptions::default());
    };
    if value.is_none() {
        return Ok(IdCompactionOptions::default());
    }
    let dict = value.downcast::<PyDict>()?;
    let parsed = IdCompactionOptions {
        enabled: get_bool_option(dict, "enabled", false)?,
        preset: get_string_option(dict, "preset", "openalex")?,
        mode: get_string_option(dict, "mode", "semantic_column_strip")?,
        description_policy: get_string_option(dict, "description_policy", "required")?,
        apply_to_excepted_raw_json: get_bool_option(dict, "apply_to_excepted_raw_json", false)?,
        collision_policy: get_string_option(dict, "collision_policy", "error")?,
        namespace_conflict_policy: get_string_option(dict, "namespace_conflict_policy", "error")?,
        rules_version: get_string_option(
            dict,
            "rules_version",
            "openalex-semantic-column-strip-v2",
        )?,
        rules_hash: get_string_option(dict, "rules_hash", "")?,
    };
    if parsed.enabled
        && (parsed.preset.as_str() != "openalex" || parsed.mode.as_str() != "semantic_column_strip")
    {
        return Err(PyRuntimeError::new_err(
            "rust-arrow id_compaction currently supports openalex/semantic_column_strip only",
        ));
    }
    Ok(parsed)
}

fn parse_options(options: &Bound<'_, PyDict>) -> PyResult<Options> {
    let mut except_keys = BTreeSet::new();
    if let Some(value) = options.get_item("except_keys")? {
        if let Ok(items) = value.downcast::<PyList>() {
            for item in items.iter() {
                let key = item.str()?.to_string();
                if !key.trim().is_empty() {
                    except_keys.insert(key.trim().to_string());
                }
            }
        }
    }

    let mut record_contexts = Vec::new();
    if let Some(value) = options.get_item("record_contexts")? {
        if let Ok(items) = value.downcast::<PyList>() {
            record_contexts.reserve(items.len());
            for item in items.iter() {
                record_contexts.push(py_to_json(&item)?);
            }
        }
    }

    Ok(Options {
        base_table: get_string_option(options, "base_table", "main")?,
        index_key: get_string_option(options, "index_key", "id")?,
        except_keys,
        excepted_expand_dict: get_bool_option(options, "excepted_expand_dict", false)?,
        sep: get_string_option(options, "sep", "__")?,
        parquet_dir: PathBuf::from(get_string_option(options, "parquet_dir", "parquet")?),
        batch_idx: get_usize_option(options, "batch_idx", 0)?,
        index_offset: get_usize_option(options, "index_offset", 0)?,
        parallel_workers: get_usize_option(options, "parallel_workers", 0)?,
        parallel_table_writes: get_bool_option(options, "parallel_table_writes", false)?,
        columnar_accumulator: get_bool_option(options, "columnar_accumulator", false)?,
        record_contexts,
        id_compaction: parse_id_compaction_options(options)?,
    })
}

fn json_dumps_python_spacing(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => {
            if *v {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Number(v) => v.to_string(),
        Value::String(v) => serde_json::to_string(v).unwrap_or_else(|_| "\"\"".to_string()),
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(json_dumps_python_spacing).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Object(map) => {
            let parts: Vec<String> = map
                .iter()
                .map(|(key, value)| {
                    let key_json =
                        serde_json::to_string(key).unwrap_or_else(|_| "\"\"".to_string());
                    format!("{key_json}: {}", json_dumps_python_spacing(value))
                })
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
    }
}

fn append_large_utf8_value(builder: &mut LargeStringBuilder, value: Option<&Value>) {
    match value {
        Some(Value::Null) | None => builder.append_null(),
        Some(Value::Bool(v)) => builder.append_value(if *v { "true" } else { "false" }),
        Some(Value::Number(v)) => builder.append_value(v.as_str()),
        Some(Value::String(v)) => builder.append_value(v),
        Some(v @ (Value::Array(_) | Value::Object(_))) => {
            builder.append_value(json_dumps_python_spacing(v))
        }
    }
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(v) => {
            if v.is_i64() || v.is_u64() {
                "int"
            } else {
                "float"
            }
        }
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

fn number_is_integer_literal(number: &Number) -> bool {
    let text = number.as_str();
    !text.contains('.') && !text.contains('e') && !text.contains('E')
}

enum JsonPathPart<'a> {
    Key(&'a str),
    Index(usize),
}

fn json_number_path(parts: &[JsonPathPart<'_>]) -> String {
    let mut out = String::from("$");
    for part in parts {
        match part {
            JsonPathPart::Key(key) => {
                out.push('.');
                out.push_str(key);
            }
            JsonPathPart::Index(index) => out.push_str(&format!("[{index}]")),
        }
    }
    out
}

fn validate_json_numbers(value: &Value) -> Result<(), String> {
    let mut path = Vec::<JsonPathPart<'_>>::new();
    validate_json_numbers_inner(value, &mut path)
}

fn validate_json_numbers_inner<'a>(
    value: &'a Value,
    path: &mut Vec<JsonPathPart<'a>>,
) -> Result<(), String> {
    match value {
        Value::Number(number) => {
            let is_integer = number_is_integer_literal(number);
            if is_integer {
                if number.as_i64().is_none() && number.as_u64().is_none() {
                    return Err(format!(
                        "integer outside supported i64/u64 range at {}: {number}",
                        json_number_path(path)
                    ));
                }
            } else if number.as_f64().is_none() {
                return Err(format!(
                    "number is not representable as f64 at {}: {number}",
                    json_number_path(path)
                ));
            }
            Ok(())
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                path.push(JsonPathPart::Index(i));
                validate_json_numbers_inner(item, path)?;
                path.pop();
            }
            Ok(())
        }
        Value::Object(map) => {
            for (key, item) in map.iter() {
                path.push(JsonPathPart::Key(key));
                validate_json_numbers_inner(item, path)?;
                path.pop();
            }
            Ok(())
        }
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(()),
    }
}

fn value_to_index(value: Option<&Value>, fallback: usize) -> Value {
    match value {
        Some(Value::Null) | None => Value::Number(Number::from(fallback as u64)),
        Some(Value::String(s)) if s.is_empty() => Value::Number(Number::from(fallback as u64)),
        Some(Value::Array(_)) | Some(Value::Object(_)) => {
            Value::Number(Number::from(fallback as u64))
        }
        Some(v) => v.clone(),
    }
}

fn flatten_dict_keep_lists(obj: &Value, except_keys: &BTreeSet<String>, sep: &str) -> Row {
    let mut out = Row::new();
    let mut stack: Vec<(&Value, String)> = vec![(obj, String::new())];
    while let Some((cur, prefix)) = stack.pop() {
        if let Value::Object(map) = cur {
            for (k, v) in map.iter() {
                let full_key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}{k}")
                };
                if except_keys.contains(k) || except_keys.contains(&full_key) {
                    continue;
                }
                match v {
                    Value::Object(_) => stack.push((v, format!("{full_key}{sep}"))),
                    Value::Array(items) => {
                        if matches!(items.first(), Some(Value::Object(_))) {
                            let mut flattened_items = Vec::new();
                            for item in items {
                                if let Value::Object(_) = item {
                                    flattened_items.push(Value::Object(
                                        flatten_dict_keep_lists(item, except_keys, sep)
                                            .into_iter()
                                            .collect(),
                                    ));
                                } else {
                                    let mut scalar_map = serde_json::Map::new();
                                    scalar_map.insert(full_key.clone(), item.clone());
                                    flattened_items.push(Value::Object(scalar_map));
                                }
                            }
                            out.insert(full_key, Value::Array(flattened_items));
                        } else {
                            out.insert(full_key, v.clone());
                        }
                    }
                    _ => {
                        out.insert(full_key, v.clone());
                    }
                }
            }
        } else if !prefix.is_empty() {
            let key = prefix.strip_suffix(sep).unwrap_or(&prefix).to_string();
            out.insert(key, cur.clone());
        }
    }
    out
}

fn prefixed_col(list_key: &str, col: &str, sep: &str) -> String {
    if col == list_key {
        col.to_string()
    } else {
        format!("{list_key}{sep}{col}")
    }
}

fn process_value_list(
    list_key: &str,
    values: &[Value],
    id_value: &Value,
    options: &Options,
    sub_rows: &mut TableRows,
) {
    if values.is_empty() {
        return;
    }
    let mut rows = Vec::new();
    if matches!(values.first(), Some(Value::Object(_))) {
        for item in values {
            let Value::Object(map) = item else {
                let mut row = Row::new();
                row.insert(options.index_key.clone(), id_value.clone());
                row.insert(list_key.to_string(), item.clone());
                rows.push(row);
                continue;
            };
            let needs_deep = map.values().any(|v| match v {
                Value::Object(_) => true,
                Value::Array(items) => matches!(items.first(), Some(Value::Object(_))),
                _ => false,
            });
            let mut row = Row::new();
            row.insert(options.index_key.clone(), id_value.clone());
            if needs_deep {
                for (col, value) in
                    flatten_dict_keep_lists(item, &options.except_keys, &options.sep)
                {
                    row.insert(prefixed_col(list_key, &col, &options.sep), value);
                }
            } else {
                for (col, value) in map.iter() {
                    if options.except_keys.contains(col) {
                        continue;
                    }
                    row.insert(prefixed_col(list_key, col, &options.sep), value.clone());
                }
            }
            rows.push(row);
        }
    } else {
        for value in values {
            let mut row = Row::new();
            row.insert(options.index_key.clone(), id_value.clone());
            row.insert(list_key.to_string(), value.clone());
            rows.push(row);
        }
    }
    if !rows.is_empty() {
        sub_rows.insert(list_key.to_string(), rows);
    }
}

fn table_for_sub(options: &Options, sub_key: &str) -> String {
    let key = sub_key.replace('.', &options.sep);
    format!("{}{}{}", options.base_table, options.sep, key)
}

fn table_for_excepted(options: &Options, ex_key: &str) -> String {
    let key = ex_key.replace('.', &options.sep);
    format!(
        "{}{}excepted{}{}",
        options.base_table, options.sep, options.sep, key
    )
}

fn build_excepted_row(
    path: &str,
    value: &Value,
    id_value: &Value,
    context: Option<&Value>,
    options: &Options,
) -> Row {
    let raw_json = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
    let mut row = Row::new();
    let stored_value = if options.excepted_expand_dict {
        value.clone()
    } else {
        match value {
            Value::Array(_) | Value::Object(_) => Value::String(raw_json.clone()),
            _ => value.clone(),
        }
    };
    row.insert("value".to_string(), stored_value);
    if options.excepted_expand_dict {
        if let Value::Object(map) = value {
            for (k, v) in map.iter() {
                row.insert(k.clone(), v.clone());
            }
        }
    }
    row.insert(options.index_key.clone(), id_value.clone());
    row.insert(
        "__except_path__".to_string(),
        Value::String(path.to_string()),
    );
    row.insert(
        "__except_raw_type__".to_string(),
        Value::String(value_type_name(value).to_string()),
    );
    row.insert("__except_raw_json__".to_string(), Value::String(raw_json));
    if let Some(Value::Object(ctx)) = context {
        for key in ["source_path", "source_member", "line_no", "record_index"] {
            if let Some(v) = ctx.get(key) {
                let out_key = match key {
                    "source_path" => "__source_path__",
                    "source_member" => "__source_member__",
                    "line_no" => "__line_no__",
                    "record_index" => "__record_index__",
                    _ => key,
                };
                row.insert(out_key.to_string(), v.clone());
            }
        }
    }
    row
}

fn build_excepted_cell_row(
    path: &str,
    value: &Value,
    id_value: &Value,
    context: Option<&Value>,
    options: &Options,
) -> CellRow {
    let raw_json = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
    let mut row = CellRow::new();
    let stored_value = if options.excepted_expand_dict {
        cell_from_value(value)
    } else {
        match value {
            Value::Array(_) | Value::Object(_) => CellValue::String(raw_json.clone()),
            _ => cell_from_value(value),
        }
    };
    row.insert("value".to_string(), stored_value);
    if options.excepted_expand_dict {
        if let Value::Object(map) = value {
            for (k, v) in map.iter() {
                row.insert(k.clone(), cell_from_value(v));
            }
        }
    }
    row.insert(options.index_key.clone(), cell_from_value(id_value));
    row.insert(
        "__except_path__".to_string(),
        CellValue::String(path.to_string()),
    );
    row.insert(
        "__except_raw_type__".to_string(),
        CellValue::String(value_type_name(value).to_string()),
    );
    row.insert(
        "__except_raw_json__".to_string(),
        CellValue::String(raw_json),
    );
    if let Some(Value::Object(ctx)) = context {
        for key in ["source_path", "source_member", "line_no", "record_index"] {
            if let Some(v) = ctx.get(key) {
                let out_key = match key {
                    "source_path" => "__source_path__",
                    "source_member" => "__source_member__",
                    "line_no" => "__line_no__",
                    "record_index" => "__record_index__",
                    _ => key,
                };
                row.insert(out_key.to_string(), cell_from_value(v));
            }
        }
    }
    row
}

fn process_value_list_columnar(
    list_key: &str,
    values: &[Value],
    id_value: &Value,
    options: &Options,
    tables: &mut ColumnarTables,
) {
    if values.is_empty() {
        return;
    }
    let table = table_for_sub(options, list_key);
    if matches!(values.first(), Some(Value::Object(_))) {
        for item in values {
            let Value::Object(map) = item else {
                let mut row = CellRow::new();
                row.insert(options.index_key.clone(), cell_from_value(id_value));
                row.insert(list_key.to_string(), cell_from_value(item));
                tables.push_row(table.clone(), row);
                continue;
            };
            let needs_deep = map.values().any(|v| match v {
                Value::Object(_) => true,
                Value::Array(items) => matches!(items.first(), Some(Value::Object(_))),
                _ => false,
            });
            let mut row = CellRow::new();
            row.insert(options.index_key.clone(), cell_from_value(id_value));
            if needs_deep {
                for (col, value) in
                    flatten_dict_keep_lists(item, &options.except_keys, &options.sep)
                {
                    row.insert(
                        prefixed_col(list_key, &col, &options.sep),
                        cell_from_value(&value),
                    );
                }
            } else {
                for (col, value) in map.iter() {
                    if options.except_keys.contains(col) {
                        continue;
                    }
                    row.insert(
                        prefixed_col(list_key, col, &options.sep),
                        cell_from_value(value),
                    );
                }
            }
            tables.push_row(table.clone(), row);
        }
    } else {
        for value in values {
            let mut row = CellRow::new();
            row.insert(options.index_key.clone(), cell_from_value(id_value));
            row.insert(list_key.to_string(), cell_from_value(value));
            tables.push_row(table.clone(), row);
        }
    }
}

fn flatten_record_columnar(
    record: &Value,
    record_index: usize,
    context: Option<&Value>,
    options: &Options,
    tables: &mut ColumnarTables,
) {
    let id_value = match record {
        Value::Object(map) if !options.except_keys.contains(&options.index_key) => {
            value_to_index(map.get(&options.index_key), record_index)
        }
        _ => Value::Number(Number::from(record_index as u64)),
    };

    let mut single = CellRow::new();
    let mut excepted_values = BTreeMap::<String, &Value>::new();
    let mut stack: Vec<(&Value, String)> = vec![(record, String::new())];

    while let Some((cur, prefix)) = stack.pop() {
        if let Value::Object(map) = cur {
            for (k, v) in map.iter() {
                let full_key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}{k}")
                };
                if options.except_keys.contains(k) || options.except_keys.contains(&full_key) {
                    excepted_values.insert(full_key, v);
                    continue;
                }
                match v {
                    Value::Object(_) => stack.push((v, format!("{full_key}{}", options.sep))),
                    Value::Array(items) => {
                        process_value_list_columnar(&full_key, items, &id_value, options, tables)
                    }
                    _ => {
                        single.insert(full_key, cell_from_value(v));
                    }
                }
            }
        } else if prefix.is_empty() {
            single.insert("root".to_string(), cell_from_value(cur));
        } else {
            let key = prefix
                .strip_suffix(&options.sep)
                .unwrap_or(&prefix)
                .to_string();
            single.insert(key, cell_from_value(cur));
        }
    }

    if !single.contains_key(&options.index_key)
        || matches!(single.get(&options.index_key), Some(CellValue::Null))
        || matches!(single.get(&options.index_key), Some(CellValue::String(s)) if s.is_empty())
    {
        single.insert(options.index_key.clone(), cell_from_value(&id_value));
    }

    tables.push_row(options.base_table.clone(), single);
    for (key, value) in excepted_values {
        let table = table_for_excepted(options, &key);
        tables.push_row(
            table,
            build_excepted_cell_row(&key, value, &id_value, context, options),
        );
    }
}

fn flatten_record(
    record: &Value,
    record_index: usize,
    context: Option<&Value>,
    options: &Options,
) -> RecordOut {
    let id_value = match record {
        Value::Object(map) if !options.except_keys.contains(&options.index_key) => {
            value_to_index(map.get(&options.index_key), record_index)
        }
        _ => Value::Number(Number::from(record_index as u64)),
    };

    let mut single = Row::new();
    let mut sub_rows = TableRows::new();
    let mut excepted_values = BTreeMap::<String, Value>::new();
    let mut stack: Vec<(&Value, String)> = vec![(record, String::new())];

    while let Some((cur, prefix)) = stack.pop() {
        if let Value::Object(map) = cur {
            for (k, v) in map.iter() {
                let full_key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}{k}")
                };
                if options.except_keys.contains(k) || options.except_keys.contains(&full_key) {
                    excepted_values.insert(full_key, v.clone());
                    continue;
                }
                match v {
                    Value::Object(_) => stack.push((v, format!("{full_key}{}", options.sep))),
                    Value::Array(items) => {
                        process_value_list(&full_key, items, &id_value, options, &mut sub_rows)
                    }
                    _ => {
                        single.insert(full_key, v.clone());
                    }
                }
            }
        } else if prefix.is_empty() {
            single.insert("root".to_string(), cur.clone());
        } else {
            let key = prefix
                .strip_suffix(&options.sep)
                .unwrap_or(&prefix)
                .to_string();
            single.insert(key, cur.clone());
        }
    }

    if !single.contains_key(&options.index_key)
        || matches!(single.get(&options.index_key), Some(Value::Null))
        || matches!(single.get(&options.index_key), Some(Value::String(s)) if s.is_empty())
    {
        single.insert(options.index_key.clone(), id_value.clone());
    }

    let mut excepted = TableRows::new();
    for (key, value) in excepted_values.iter() {
        let table = table_for_excepted(options, key);
        excepted
            .entry(table)
            .or_default()
            .push(build_excepted_row(key, value, &id_value, context, options));
    }

    RecordOut {
        ok: true,
        main: single,
        subs: sub_rows,
        excepted,
        error: None,
    }
}

fn slug(value: &str, max_len: usize) -> String {
    let mut out: String = value
        .chars()
        .map(|ch| {
            if ch.is_alphanumeric() || matches!(ch, '_' | '-' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    out = out.trim_matches(|ch| ch == '.' || ch == '_').to_string();
    if out.is_empty() {
        out = "unknown".to_string();
    }
    if out.chars().count() > max_len {
        out = out.chars().take(max_len).collect();
    }
    out
}

fn absolute_no_resolve(path: &Path) -> PyResult<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .join(path))
    }
}

fn assert_no_symlink_components(path: &Path, purpose: &str) -> PyResult<PathBuf> {
    let path_abs = absolute_no_resolve(path)?;
    let mut current = PathBuf::new();
    let mut normalized_parts: Vec<std::ffi::OsString> = Vec::new();
    let mut anchor = PathBuf::new();
    for component in path_abs.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {
                anchor.push(component.as_os_str());
                current.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                normalized_parts.pop();
                current = anchor.clone();
                for part in normalized_parts.iter() {
                    current.push(part);
                }
            }
            Component::Normal(part) => {
                current.push(part);
                match fs::symlink_metadata(&current) {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        return Err(PyRuntimeError::new_err(format!(
                            "{purpose} path contains a symlink component: {}",
                            current.display()
                        )));
                    }
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(PyRuntimeError::new_err(format!(
                            "failed to inspect {purpose} path component {}: {e}",
                            current.display()
                        )));
                    }
                }
                normalized_parts.push(part.to_os_string());
            }
        }
    }
    let mut normalized = anchor;
    for part in normalized_parts {
        normalized.push(part);
    }
    Ok(normalized)
}

fn prepare_parquet_root(root: &Path) -> PyResult<PathBuf> {
    let root_abs = assert_no_symlink_components(root, "rust-arrow parquet root")?;
    fs::create_dir_all(&root_abs).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let root_abs = assert_no_symlink_components(&root_abs, "rust-arrow parquet root")?;
    let meta =
        fs::symlink_metadata(&root_abs).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(PyRuntimeError::new_err(format!(
            "rust-arrow parquet root is not a safe directory: {}",
            root_abs.display()
        )));
    }
    root_abs
        .canonicalize()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

fn prepare_table_dir(root: &Path, table: &str) -> PyResult<PathBuf> {
    let table_dir = root.join(slug(table, 120));
    assert_no_symlink_components(&table_dir, "rust-arrow parquet table")?;
    fs::create_dir_all(&table_dir).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let table_dir = assert_no_symlink_components(&table_dir, "rust-arrow parquet table")?;
    let table_canon = table_dir
        .canonicalize()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let root_canon = root
        .canonicalize()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    if !table_canon.starts_with(&root_canon) {
        return Err(PyRuntimeError::new_err(format!(
            "rust-arrow parquet table path resolves outside the parquet root: {}",
            table_dir.display()
        )));
    }
    Ok(table_canon)
}

fn path_occupied(path: &Path) -> PyResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(PyRuntimeError::new_err(format!(
            "failed to inspect rust-arrow parquet output path {}: {e}",
            path.display()
        ))),
    }
}

fn output_path(root: &Path, table: &str, batch_idx: usize) -> PyResult<PathBuf> {
    let table_dir = prepare_table_dir(root, table)?;
    let base = format!("b{batch_idx:06}");
    for i in 0..100_000usize {
        let name = if i == 0 {
            format!("{base}.parquet")
        } else {
            format!("{base}_{i}.parquet")
        };
        let path = table_dir.join(name);
        if !path_occupied(&path)? {
            return Ok(path);
        }
    }
    Err(PyRuntimeError::new_err(format!(
        "failed to allocate rust-arrow parquet output path for table {table}, batch {batch_idx}: too many collisions"
    )))
}

fn temp_output_file(table_dir: &Path, batch_idx: usize) -> PyResult<(PathBuf, fs::File)> {
    let pid = std::process::id();
    for i in 0..100_000usize {
        let path = table_dir.join(format!(".b{batch_idx:06}.{pid}.{i}.tmp"));
        if path_occupied(&path)? {
            continue;
        }
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(PyRuntimeError::new_err(e.to_string())),
        }
    }
    Err(PyRuntimeError::new_err(format!(
        "failed to allocate rust-arrow temporary parquet output path for batch {batch_idx}: too many collisions"
    )))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnKind {
    Bool,
    Int64,
    Float64,
    LargeUtf8,
}

fn value_kind(value: &Value) -> Option<ColumnKind> {
    match value {
        Value::Null => None,
        Value::Bool(_) => Some(ColumnKind::Bool),
        Value::Number(n) => {
            if n.is_i64() {
                Some(ColumnKind::Int64)
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    Some(ColumnKind::Int64)
                } else {
                    Some(ColumnKind::LargeUtf8)
                }
            } else if number_is_integer_literal(n) {
                Some(ColumnKind::LargeUtf8)
            } else {
                Some(ColumnKind::Float64)
            }
        }
        Value::String(_) | Value::Array(_) | Value::Object(_) => Some(ColumnKind::LargeUtf8),
    }
}

fn merge_kind(left: Option<ColumnKind>, right: Option<ColumnKind>) -> Option<ColumnKind> {
    match (left, right) {
        (None, value) => value,
        (value, None) => value,
        (Some(a), Some(b)) if a == b => Some(a),
        (Some(ColumnKind::Int64), Some(ColumnKind::Float64))
        | (Some(ColumnKind::Float64), Some(ColumnKind::Int64)) => Some(ColumnKind::Float64),
        _ => Some(ColumnKind::LargeUtf8),
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok())),
        _ => None,
    }
}

#[derive(Clone)]
enum CellValue {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
}

type CellRow = BTreeMap<String, CellValue>;

fn cell_from_value(value: &Value) -> CellValue {
    match value {
        Value::Null => CellValue::Null,
        Value::Bool(v) => CellValue::Bool(*v),
        Value::Number(v) => CellValue::Number(v.clone()),
        Value::String(v) => CellValue::String(v.clone()),
        Value::Array(_) | Value::Object(_) => CellValue::String(json_dumps_python_spacing(value)),
    }
}

fn cell_kind(value: &CellValue) -> Option<ColumnKind> {
    match value {
        CellValue::Null => None,
        CellValue::Bool(_) => Some(ColumnKind::Bool),
        CellValue::Number(n) => {
            if n.is_i64() {
                Some(ColumnKind::Int64)
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    Some(ColumnKind::Int64)
                } else {
                    Some(ColumnKind::LargeUtf8)
                }
            } else if number_is_integer_literal(n) {
                Some(ColumnKind::LargeUtf8)
            } else {
                Some(ColumnKind::Float64)
            }
        }
        CellValue::String(_) => Some(ColumnKind::LargeUtf8),
    }
}

fn cell_as_i64(value: &CellValue) -> Option<i64> {
    match value {
        CellValue::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok())),
        _ => None,
    }
}

fn append_large_utf8_cell(builder: &mut LargeStringBuilder, value: Option<&CellValue>) {
    match value {
        Some(CellValue::Null) | None => builder.append_null(),
        Some(CellValue::Bool(v)) => builder.append_value(if *v { "true" } else { "false" }),
        Some(CellValue::Number(v)) => builder.append_value(v.as_str()),
        Some(CellValue::String(v)) => builder.append_value(v),
    }
}

struct ColumnAccumulator {
    kind: Option<ColumnKind>,
    entries: Vec<(usize, CellValue)>,
}

impl ColumnAccumulator {
    fn new() -> Self {
        Self {
            kind: None,
            entries: Vec::new(),
        }
    }

    fn push(&mut self, row_index: usize, value: CellValue) {
        self.kind = merge_kind(self.kind, cell_kind(&value));
        self.entries.push((row_index, value));
    }
}

struct TableAccumulator {
    rows: usize,
    columns: BTreeMap<String, ColumnAccumulator>,
}

impl TableAccumulator {
    fn new() -> Self {
        Self {
            rows: 0,
            columns: BTreeMap::new(),
        }
    }

    fn push_row(&mut self, row: CellRow) {
        let row_index = self.rows;
        self.rows += 1;
        for (column, value) in row {
            self.columns
                .entry(column)
                .or_insert_with(ColumnAccumulator::new)
                .push(row_index, value);
        }
    }

    fn merge_from(&mut self, other: TableAccumulator) {
        let offset = self.rows;
        self.rows += other.rows;
        for (column, mut incoming) in other.columns {
            let target = self
                .columns
                .entry(column)
                .or_insert_with(ColumnAccumulator::new);
            target.kind = merge_kind(target.kind, incoming.kind);
            target.entries.reserve(incoming.entries.len());
            for (row_index, value) in incoming.entries.drain(..) {
                target.entries.push((row_index + offset, value));
            }
        }
    }
}

struct ColumnarTables {
    tables: BTreeMap<String, TableAccumulator>,
}

impl ColumnarTables {
    fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    fn push_row(&mut self, table: String, row: CellRow) {
        self.tables
            .entry(table)
            .or_insert_with(TableAccumulator::new)
            .push_row(row);
    }

    fn merge_from(&mut self, other: ColumnarTables) {
        for (table, incoming) in other.tables {
            self.tables
                .entry(table)
                .or_insert_with(TableAccumulator::new)
                .merge_from(incoming);
        }
    }
}

fn entity_alias(value: &str) -> Option<&'static str> {
    match value {
        "author" | "authors" => Some("author"),
        "institution" | "institutions" => Some("institution"),
        "source" | "sources" => Some("source"),
        "work" | "works" => Some("work"),
        "concept" | "concepts" => Some("concept"),
        "topic" | "topics" | "topic_share" | "primary_topic" => Some("topic"),
        "field" | "fields" => Some("field"),
        "subfield" | "subfields" => Some("subfield"),
        "domain" | "domains" => Some("domain"),
        "funder" | "funders" => Some("funder"),
        "publisher" | "publishers" => Some("publisher"),
        "organization" | "organisations" | "organizations" => Some("organization"),
        _ => None,
    }
}

fn openalex_value_column(value: &str) -> Option<&'static str> {
    match value {
        "referenced_works" => Some("referenced_work"),
        "related_works" => Some("related_work"),
        "corresponding_author_ids" => Some("corresponding_author"),
        "corresponding_institution_ids" => Some("corresponding_institution"),
        "host_organization" => Some("host_organization"),
        "host_organization_lineage" => Some("host_organization_lineage"),
        _ => None,
    }
}

fn split_column_parts<'a>(column: &'a str, sep: &str) -> Vec<&'a str> {
    if !sep.is_empty() && column.contains(sep) {
        column.split(sep).collect()
    } else {
        vec![column]
    }
}

fn replace_last(parts: &[&str], value: &str, sep: &str) -> String {
    let mut out: Vec<String> = parts.iter().map(|v| (*v).to_string()).collect();
    if let Some(last) = out.last_mut() {
        *last = value.to_string();
    }
    out.join(sep)
}

fn openalex_column_name(
    column: &str,
    index_key: &str,
    sep: &str,
) -> (Option<String>, Option<String>) {
    if column == index_key {
        return (Some(column.to_string()), None);
    }

    let parts = split_column_parts(column, sep);
    let last = parts.last().copied().unwrap_or(column);

    if last.ends_with("_openalex_id") && last.len() > "_openalex_id".len() {
        let entity = last[..last.len() - "_openalex_id".len()].to_string();
        return (Some(column.to_string()), Some(entity));
    }

    if last == "openalex" {
        return (Some(replace_last(&parts, "openalex_id", sep)), None);
    }

    if last == "id" && parts.len() >= 2 {
        let prev = parts[parts.len() - 2];
        if let Some(entity) = entity_alias(prev) {
            let mut out: Vec<String> = parts[..parts.len() - 2]
                .iter()
                .map(|v| (*v).to_string())
                .collect();
            out.push(format!("{entity}_openalex_id"));
            return (Some(out.join(sep)), Some(entity.to_string()));
        }
    }

    if let Some(entity) = openalex_value_column(last) {
        return (
            Some(replace_last(&parts, &format!("{entity}_openalex_id"), sep)),
            Some(entity.to_string()),
        );
    }

    if last.ends_with("_ids") && last.len() > 4 {
        let stem = &last[..last.len() - 4];
        if let Some(entity) = entity_alias(stem) {
            return (
                Some(replace_last(&parts, &format!("{entity}_openalex_id"), sep)),
                Some(entity.to_string()),
            );
        }
    }

    if last.ends_with("_id") && last.len() > 3 {
        let stem = &last[..last.len() - 3];
        if let Some(entity) = entity_alias(stem) {
            return (
                Some(replace_last(&parts, &format!("{entity}_openalex_id"), sep)),
                Some(entity.to_string()),
            );
        }
    }

    if matches!(last, "host_organization" | "host_organization_lineage") {
        return (
            Some(replace_last(&parts, &format!("{last}_openalex_id"), sep)),
            Some(last.to_string()),
        );
    }

    (None, None)
}

fn column_namespace_mapping(
    column: &str,
    index_key: &str,
    sep: &str,
) -> (Option<String>, Option<&'static str>, Option<String>) {
    let parts = split_column_parts(column, sep);
    let last = parts.last().copied().unwrap_or(column);

    let (new_col, entity) = openalex_column_name(column, index_key, sep);
    if new_col.is_some() {
        return (new_col, Some("openalex"), entity);
    }

    match last {
        "ror" => (
            Some(replace_last(&parts, "ror_id", sep)),
            Some("ror"),
            Some("ror".to_string()),
        ),
        "ror_id" => (
            Some(column.to_string()),
            Some("ror"),
            Some("ror".to_string()),
        ),
        "doi" => (
            Some(replace_last(&parts, "doi_id", sep)),
            Some("doi"),
            Some("doi".to_string()),
        ),
        "doi_id" => (
            Some(column.to_string()),
            Some("doi"),
            Some("doi".to_string()),
        ),
        "orcid" => (
            Some(replace_last(&parts, "orcid_id", sep)),
            Some("orcid"),
            Some("orcid".to_string()),
        ),
        "orcid_id" => (
            Some(column.to_string()),
            Some("orcid"),
            Some("orcid".to_string()),
        ),
        _ => (None, None, None),
    }
}

fn strip_prefix_case_insensitive(
    value: &str,
    prefixes: &[&'static str],
) -> Option<(&'static str, String)> {
    let lower = value.to_lowercase();
    for prefix in prefixes {
        if lower.starts_with(prefix) {
            let tail = value[prefix.len()..].to_string();
            if !tail.is_empty() {
                return Some((*prefix, tail));
            }
        }
    }
    None
}

fn namespace_for_value(
    value: &Value,
) -> (Option<&'static str>, Option<&'static str>, Option<String>) {
    let Value::String(text) = value else {
        return (None, None, None);
    };
    for (namespace, prefixes) in [
        (
            "openalex",
            &["https://openalex.org/", "http://openalex.org/"][..],
        ),
        ("ror", &["https://ror.org/", "http://ror.org/"][..]),
        ("doi", &["https://doi.org/", "http://doi.org/"][..]),
        ("orcid", &["https://orcid.org/", "http://orcid.org/"][..]),
    ] {
        if let Some((prefix, tail)) = strip_prefix_case_insensitive(text, prefixes) {
            return (Some(namespace), Some(prefix), Some(tail));
        }
    }
    (None, None, None)
}

fn default_prefix(namespace: &str) -> &'static str {
    match namespace {
        "openalex" => "https://openalex.org/",
        "ror" => "https://ror.org/",
        "doi" => "https://doi.org/",
        "orcid" => "https://orcid.org/",
        _ => "",
    }
}

fn capitalize_word(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str().to_lowercase()),
        None => String::new(),
    }
}

fn label_for_entity(entity: Option<&str>, namespace: &str) -> String {
    if let Some(entity) = entity {
        return entity
            .split('_')
            .map(capitalize_word)
            .collect::<Vec<String>>()
            .join(" ");
    }
    capitalize_word(namespace)
}

fn id_description(namespace: &str, entity: Option<&str>, removed_prefix: &str) -> String {
    match namespace {
        "openalex" => {
            if entity.is_none() {
                format!(
                    "OpenAlex ID. Original URL prefix {removed_prefix} removed during JSON parsing."
                )
            } else {
                let label = label_for_entity(entity, namespace);
                format!(
                    "OpenAlex {label} ID. Original URL prefix {removed_prefix} removed during JSON parsing."
                )
            }
        }
        "ror" => {
            format!("ROR ID. Original URL prefix {removed_prefix} removed during JSON parsing.")
        }
        "doi" => format!("DOI. Original URL prefix {removed_prefix} removed during JSON parsing."),
        "orcid" => {
            format!("ORCID ID. Original URL prefix {removed_prefix} removed during JSON parsing.")
        }
        _ => format!(
            "{namespace} ID. Original URL prefix {removed_prefix} removed during JSON parsing."
        ),
    }
}

fn is_blank_id_value(value: &Value) -> bool {
    matches!(value, Value::Null) || matches!(value, Value::String(s) if s.is_empty())
}

impl IdCompactionState {
    fn record(&mut self, meta: IdColumnMeta) {
        let count_key = format!("{}.{}", meta.table, meta.new_column);
        *self.counts.entry(count_key).or_insert(0) += 1;
        let entry_key = format!(
            "{}\0{}\0{}\0{}",
            meta.table, meta.original_column, meta.new_column, meta.removed_prefix
        );
        self.columns
            .entry(entry_key)
            .and_modify(|entry| entry.count += 1)
            .or_insert_with(|| {
                let mut entry = meta;
                entry.count = 1;
                entry
            });
    }

    fn compact_field(
        &mut self,
        options: &Options,
        table_name: &str,
        column: &str,
        value: &Value,
    ) -> PyResult<(String, Value, Option<IdColumnMeta>)> {
        if column == "__except_raw_json__" && !options.id_compaction.apply_to_excepted_raw_json {
            return Ok((column.to_string(), value.clone(), None));
        }
        if options.id_compaction.preset.as_str() != "openalex"
            || options.id_compaction.mode.as_str() != "semantic_column_strip"
        {
            return Ok((column.to_string(), value.clone(), None));
        }

        let (semantic_column, expected_namespace, entity) =
            column_namespace_mapping(column, &options.index_key, &options.sep);
        let (namespace, removed_prefix, tail) = namespace_for_value(value);

        let Some(expected_namespace) = expected_namespace else {
            if namespace.is_some() {
                let key = format!("{table_name}.{column}");
                *self.ambiguous_counts.entry(key).or_insert(0) += 1;
            }
            return Ok((column.to_string(), value.clone(), None));
        };

        let Some(semantic_column) = semantic_column else {
            if namespace.is_some() {
                let key = format!("{table_name}.{column}");
                *self.ambiguous_counts.entry(key).or_insert(0) += 1;
            }
            return Ok((column.to_string(), value.clone(), None));
        };

        if let Some(namespace) = namespace {
            if namespace != expected_namespace {
                let conflict_key = format!("{table_name}.{column}");
                *self
                    .namespace_conflict_counts
                    .entry(conflict_key.clone())
                    .or_insert(0) += 1;
                if options.id_compaction.namespace_conflict_policy.as_str() == "error" {
                    return Err(PyRuntimeError::new_err(format!(
                        "id compaction namespace conflict at {conflict_key}: column expects {expected_namespace:?}, value uses {namespace:?}"
                    )));
                }
                return Ok((column.to_string(), value.clone(), None));
            }
        }

        if namespace.is_none() || removed_prefix.is_none() || tail.is_none() {
            if semantic_column == column {
                return Ok((column.to_string(), value.clone(), None));
            }
            let removed_prefix_default = default_prefix(expected_namespace).to_string();
            let description = id_description(
                expected_namespace,
                entity.as_deref(),
                &removed_prefix_default,
            );
            let meta = IdColumnMeta {
                table: table_name.to_string(),
                original_column: column.to_string(),
                new_column: semantic_column.clone(),
                namespace: expected_namespace.to_string(),
                entity,
                removed_prefix: removed_prefix_default,
                description,
                count: 0,
            };
            return Ok((semantic_column, value.clone(), Some(meta)));
        }

        let removed_prefix = removed_prefix.unwrap_or_default().to_string();
        let tail = tail.unwrap_or_default();
        let description = id_description(expected_namespace, entity.as_deref(), &removed_prefix);
        let meta = IdColumnMeta {
            table: table_name.to_string(),
            original_column: column.to_string(),
            new_column: semantic_column.clone(),
            namespace: expected_namespace.to_string(),
            entity,
            removed_prefix,
            description,
            count: 0,
        };
        Ok((semantic_column, Value::String(tail), Some(meta)))
    }

    fn compact_row(&mut self, options: &Options, table_name: &str, row: &Row) -> PyResult<Row> {
        if !options.id_compaction.enabled {
            return Ok(row.clone());
        }

        let mut out = Row::new();
        let mut origins: HashMap<String, String> = HashMap::new();

        for (key, value) in row.iter() {
            let (new_key, new_value, meta) = self.compact_field(options, table_name, key, value)?;
            if let Some(meta) = meta {
                self.record(meta);
            }

            if out.contains_key(&new_key) {
                if is_blank_id_value(&new_value) {
                    continue;
                }
                if out.get(&new_key).is_some_and(is_blank_id_value) {
                    out.insert(new_key.clone(), new_value);
                    origins.insert(new_key, key.clone());
                } else if out.get(&new_key) != Some(&new_value) {
                    let collision_key = format!("{table_name}.{new_key}");
                    *self
                        .collision_counts
                        .entry(collision_key.clone())
                        .or_insert(0) += 1;
                    if options.id_compaction.collision_policy.as_str() == "error" {
                        let previous_key = origins.get(&new_key).unwrap_or(&new_key);
                        return Err(PyRuntimeError::new_err(format!(
                            "id compaction collision at {collision_key}: {key:?} and {previous_key:?} map to existing output column {new_key:?}"
                        )));
                    }
                    out.insert(key.clone(), value.clone());
                }
                continue;
            }

            out.insert(new_key.clone(), new_value);
            origins.insert(new_key, key.clone());
        }
        Ok(out)
    }

    fn set_count_map(
        py: Python<'_>,
        parent: &Bound<'_, PyDict>,
        key: &str,
        values: &BTreeMap<String, usize>,
    ) -> PyResult<()> {
        let out = PyDict::new_bound(py);
        for (name, count) in values.iter() {
            out.set_item(name, *count)?;
        }
        parent.set_item(key, out)
    }

    fn to_py(&self, py: Python<'_>, options: &IdCompactionOptions) -> PyResult<PyObject> {
        let out = PyDict::new_bound(py);
        out.set_item("enabled", options.enabled)?;
        out.set_item("preset", &options.preset)?;
        out.set_item("mode", &options.mode)?;
        out.set_item("description_policy", &options.description_policy)?;
        out.set_item(
            "apply_to_excepted_raw_json",
            options.apply_to_excepted_raw_json,
        )?;
        out.set_item("rules_version", &options.rules_version)?;
        out.set_item("rules_hash", &options.rules_hash)?;

        let mut columns: Vec<&IdColumnMeta> = self.columns.values().collect();
        columns.sort_by(|a, b| {
            (
                a.table.as_str(),
                a.new_column.as_str(),
                a.original_column.as_str(),
            )
                .cmp(&(
                    b.table.as_str(),
                    b.new_column.as_str(),
                    b.original_column.as_str(),
                ))
        });
        let column_items = PyList::empty_bound(py);
        for meta in columns {
            let item = PyDict::new_bound(py);
            item.set_item("table", &meta.table)?;
            item.set_item("original_column", &meta.original_column)?;
            item.set_item("new_column", &meta.new_column)?;
            item.set_item("namespace", &meta.namespace)?;
            match &meta.entity {
                Some(entity) => item.set_item("entity", entity)?,
                None => item.set_item("entity", py.None())?,
            }
            item.set_item("removed_prefix", &meta.removed_prefix)?;
            item.set_item("description", &meta.description)?;
            item.set_item("count", meta.count)?;
            column_items.append(item)?;
        }
        out.set_item("columns", column_items)?;
        Self::set_count_map(py, &out, "counts", &self.counts)?;
        Self::set_count_map(py, &out, "ambiguous_columns", &self.ambiguous_counts)?;
        Self::set_count_map(py, &out, "collisions", &self.collision_counts)?;
        Self::set_count_map(
            py,
            &out,
            "namespace_conflicts",
            &self.namespace_conflict_counts,
        )?;
        Ok(out.into())
    }
}

fn write_table(
    root: &Path,
    table: &str,
    rows: &[Row],
    batch_idx: usize,
    index_key: &str,
) -> PyResult<(PathBuf, Vec<String>, usize)> {
    let mut inferred_kinds: BTreeMap<String, Option<ColumnKind>> = BTreeMap::new();
    for row in rows {
        for (key, value) in row.iter() {
            let entry = inferred_kinds.entry(key.clone()).or_insert(None);
            *entry = merge_kind(*entry, value_kind(value));
        }
    }
    let mut cols: Vec<String> = inferred_kinds.keys().cloned().collect();
    if let Some(pos) = cols.iter().position(|c| c == index_key) {
        let index_col = cols.remove(pos);
        cols.insert(0, index_col);
    }
    if cols.is_empty() {
        return Err(PyRuntimeError::new_err(format!(
            "table {table} has no columns"
        )));
    }

    let fields: Vec<Field> = cols
        .iter()
        .map(|c| {
            let dtype = match inferred_kinds
                .get(c)
                .and_then(|kind| *kind)
                .unwrap_or(ColumnKind::LargeUtf8)
            {
                ColumnKind::Bool => DataType::Boolean,
                ColumnKind::Int64 => DataType::Int64,
                ColumnKind::Float64 => DataType::Float64,
                ColumnKind::LargeUtf8 => DataType::LargeUtf8,
            };
            Field::new(c, dtype, true)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for col in cols.iter() {
        match inferred_kinds
            .get(col)
            .and_then(|kind| *kind)
            .unwrap_or(ColumnKind::LargeUtf8)
        {
            ColumnKind::Bool => {
                let mut builder = BooleanBuilder::with_capacity(rows.len());
                for row in rows {
                    match row.get(col) {
                        Some(Value::Bool(value)) => builder.append_value(*value),
                        _ => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            ColumnKind::Int64 => {
                let mut builder = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    match row.get(col).and_then(value_as_i64) {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            ColumnKind::Float64 => {
                let mut builder = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    match row.get(col).and_then(|v| match v {
                        Value::Number(n) => n.as_f64(),
                        _ => None,
                    }) {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            ColumnKind::LargeUtf8 => {
                let mut builder = LargeStringBuilder::with_capacity(rows.len(), 0);
                for row in rows {
                    append_large_utf8_value(&mut builder, row.get(col));
                }
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let mut path = output_path(root, table, batch_idx)?;
    let table_dir = path
        .parent()
        .ok_or_else(|| PyRuntimeError::new_err("rust-arrow output path has no parent"))?;
    let (tmp_path, file) = temp_output_file(table_dir, batch_idx)?;
    let write_result = (|| -> PyResult<()> {
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        writer
            .close()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    })();
    if let Err(err) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }

    loop {
        match fs::hard_link(&tmp_path, &path) {
            Ok(()) => break,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                path = output_path(root, table, batch_idx)?;
            }
            Err(e) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(PyRuntimeError::new_err(e.to_string()));
            }
        }
    }
    let _ = fs::remove_file(&tmp_path);
    Ok((path, cols, rows.len()))
}

fn write_columnar_table(
    root: &Path,
    table: &str,
    accumulator: &TableAccumulator,
    batch_idx: usize,
    index_key: &str,
) -> PyResult<(PathBuf, Vec<String>, usize)> {
    let mut cols: Vec<String> = accumulator.columns.keys().cloned().collect();
    if let Some(pos) = cols.iter().position(|c| c == index_key) {
        let index_col = cols.remove(pos);
        cols.insert(0, index_col);
    }
    if cols.is_empty() {
        return Err(PyRuntimeError::new_err(format!(
            "table {table} has no columns"
        )));
    }

    let fields: Vec<Field> = cols
        .iter()
        .map(|c| {
            let dtype = match accumulator
                .columns
                .get(c)
                .and_then(|column| column.kind)
                .unwrap_or(ColumnKind::LargeUtf8)
            {
                ColumnKind::Bool => DataType::Boolean,
                ColumnKind::Int64 => DataType::Int64,
                ColumnKind::Float64 => DataType::Float64,
                ColumnKind::LargeUtf8 => DataType::LargeUtf8,
            };
            Field::new(c, dtype, true)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for col in cols.iter() {
        let column = accumulator.columns.get(col).ok_or_else(|| {
            PyRuntimeError::new_err(format!(
                "missing column accumulator for table {table}.{col}"
            ))
        })?;
        match column.kind.unwrap_or(ColumnKind::LargeUtf8) {
            ColumnKind::Bool => {
                let mut builder = BooleanBuilder::with_capacity(accumulator.rows);
                let mut next_row = 0usize;
                for (row_index, value) in column.entries.iter() {
                    while next_row < *row_index {
                        builder.append_null();
                        next_row += 1;
                    }
                    match value {
                        CellValue::Bool(v) => builder.append_value(*v),
                        _ => builder.append_null(),
                    }
                    next_row = *row_index + 1;
                }
                while next_row < accumulator.rows {
                    builder.append_null();
                    next_row += 1;
                }
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            ColumnKind::Int64 => {
                let mut builder = Int64Builder::with_capacity(accumulator.rows);
                let mut next_row = 0usize;
                for (row_index, value) in column.entries.iter() {
                    while next_row < *row_index {
                        builder.append_null();
                        next_row += 1;
                    }
                    match cell_as_i64(value) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                    next_row = *row_index + 1;
                }
                while next_row < accumulator.rows {
                    builder.append_null();
                    next_row += 1;
                }
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            ColumnKind::Float64 => {
                let mut builder = Float64Builder::with_capacity(accumulator.rows);
                let mut next_row = 0usize;
                for (row_index, value) in column.entries.iter() {
                    while next_row < *row_index {
                        builder.append_null();
                        next_row += 1;
                    }
                    match value {
                        CellValue::Number(n) => match n.as_f64() {
                            Some(v) => builder.append_value(v),
                            None => builder.append_null(),
                        },
                        _ => builder.append_null(),
                    }
                    next_row = *row_index + 1;
                }
                while next_row < accumulator.rows {
                    builder.append_null();
                    next_row += 1;
                }
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            ColumnKind::LargeUtf8 => {
                let mut builder = LargeStringBuilder::with_capacity(accumulator.rows, 0);
                let mut next_row = 0usize;
                for (row_index, value) in column.entries.iter() {
                    while next_row < *row_index {
                        builder.append_null();
                        next_row += 1;
                    }
                    append_large_utf8_cell(&mut builder, Some(value));
                    next_row = *row_index + 1;
                }
                while next_row < accumulator.rows {
                    builder.append_null();
                    next_row += 1;
                }
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let mut path = output_path(root, table, batch_idx)?;
    let table_dir = path
        .parent()
        .ok_or_else(|| PyRuntimeError::new_err("rust-arrow output path has no parent"))?;
    let (tmp_path, file) = temp_output_file(table_dir, batch_idx)?;
    let write_result = (|| -> PyResult<()> {
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        writer
            .close()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    })();
    if let Err(err) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }

    loop {
        match fs::hard_link(&tmp_path, &path) {
            Ok(()) => break,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                path = output_path(root, table, batch_idx)?;
            }
            Err(e) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(PyRuntimeError::new_err(e.to_string()));
            }
        }
    }
    let _ = fs::remove_file(&tmp_path);
    Ok((path, cols, accumulator.rows))
}

fn sql_quote_ident(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn get_required_string_option(options: &Bound<'_, PyDict>, key: &str) -> PyResult<String> {
    match options.get_item(key)? {
        Some(value) if !value.is_none() => {
            let text = value.extract::<String>()?;
            if text.trim().is_empty() {
                Err(PyRuntimeError::new_err(format!(
                    "missing required Rust DB load option: {key}"
                )))
            } else {
                Ok(text)
            }
        }
        _ => Err(PyRuntimeError::new_err(format!(
            "missing required Rust DB load option: {key}"
        ))),
    }
}

enum MysqlArrayView<'a> {
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
}

impl<'a> MysqlArrayView<'a> {
    fn from_array(array: &'a ArrayRef, column: &str) -> PyResult<Self> {
        match array.data_type() {
            DataType::Boolean => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast boolean array"))?;
                Ok(Self::Boolean(arr))
            }
            DataType::Int8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast int8 array"))?;
                Ok(Self::Int8(arr))
            }
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast int16 array"))?;
                Ok(Self::Int16(arr))
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast int32 array"))?;
                Ok(Self::Int32(arr))
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast int64 array"))?;
                Ok(Self::Int64(arr))
            }
            DataType::UInt8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast uint8 array"))?;
                Ok(Self::UInt8(arr))
            }
            DataType::UInt16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast uint16 array"))?;
                Ok(Self::UInt16(arr))
            }
            DataType::UInt32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast uint32 array"))?;
                Ok(Self::UInt32(arr))
            }
            DataType::UInt64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast uint64 array"))?;
                Ok(Self::UInt64(arr))
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast float32 array"))?;
                Ok(Self::Float32(arr))
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast float64 array"))?;
                Ok(Self::Float64(arr))
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast utf8 array"))?;
                Ok(Self::Utf8(arr))
            }
            DataType::LargeUtf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        PyRuntimeError::new_err("failed to downcast large utf8 array")
                    })?;
                Ok(Self::LargeUtf8(arr))
            }
            DataType::Binary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| PyRuntimeError::new_err("failed to downcast binary array"))?;
                Ok(Self::Binary(arr))
            }
            DataType::LargeBinary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        PyRuntimeError::new_err("failed to downcast large binary array")
                    })?;
                Ok(Self::LargeBinary(arr))
            }
            dtype => Err(PyRuntimeError::new_err(format!(
            "rust mysql loader does not support parquet column {column} with Arrow type {dtype:?}"
        ))),
        }
    }

    fn mysql_value(&self, row: usize) -> mysql::Value {
        match self {
            Self::Boolean(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Int(if arr.value(row) { 1 } else { 0 })
                }
            }
            Self::Int8(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Int(arr.value(row) as i64)
                }
            }
            Self::Int16(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Int(arr.value(row) as i64)
                }
            }
            Self::Int32(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Int(arr.value(row) as i64)
                }
            }
            Self::Int64(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Int(arr.value(row))
                }
            }
            Self::UInt8(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::UInt(arr.value(row) as u64)
                }
            }
            Self::UInt16(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::UInt(arr.value(row) as u64)
                }
            }
            Self::UInt32(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::UInt(arr.value(row) as u64)
                }
            }
            Self::UInt64(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::UInt(arr.value(row))
                }
            }
            Self::Float32(arr) => {
                if arr.is_null(row) {
                    return mysql::Value::NULL;
                }
                let value = arr.value(row);
                if value.is_nan() {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Double(value as f64)
                }
            }
            Self::Float64(arr) => {
                if arr.is_null(row) {
                    return mysql::Value::NULL;
                }
                let value = arr.value(row);
                if value.is_nan() {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Double(value)
                }
            }
            Self::Utf8(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Bytes(arr.value(row).as_bytes().to_vec())
                }
            }
            Self::LargeUtf8(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Bytes(arr.value(row).as_bytes().to_vec())
                }
            }
            Self::Binary(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Bytes(arr.value(row).to_vec())
                }
            }
            Self::LargeBinary(arr) => {
                if arr.is_null(row) {
                    mysql::Value::NULL
                } else {
                    mysql::Value::Bytes(arr.value(row).to_vec())
                }
            }
        }
    }
}

struct RustMysqlLoadStats {
    files_loaded: usize,
    tables_loaded: usize,
    rows_loaded: usize,
    table_meta: Vec<RustMysqlTableMeta>,
}

impl RustMysqlLoadStats {
    fn new() -> Self {
        Self {
            files_loaded: 0,
            tables_loaded: 0,
            rows_loaded: 0,
            table_meta: Vec::new(),
        }
    }
}

fn rust_mysql_context_error(
    table_sql: &str,
    path: &Path,
    action: &str,
    err: impl std::fmt::Display,
) -> PyErr {
    PyRuntimeError::new_err(format!(
        "Rust MySQL loader failed while {action} (table={}, path={}): {err}",
        table_sql,
        path.display()
    ))
}

fn parse_rust_mysql_tables(table_items: &Bound<'_, PyList>) -> PyResult<Vec<RustMysqlTable>> {
    let mut tables = Vec::<RustMysqlTable>::with_capacity(table_items.len());
    for item in table_items.iter() {
        let info = item.downcast::<PyDict>()?;
        let path = PathBuf::from(get_required_string_option(info, "path")?);
        let table_sql = get_required_string_option(info, "table_sql")?;
        let columns_original_py = info.get_item("columns_original")?.ok_or_else(|| {
            PyRuntimeError::new_err("missing columns_original for Rust MySQL loader")
        })?;
        let columns_sql_py = info
            .get_item("columns_sql")?
            .ok_or_else(|| PyRuntimeError::new_err("missing columns_sql for Rust MySQL loader"))?;
        let columns_original_list = columns_original_py.downcast::<PyList>()?;
        let columns_sql_list = columns_sql_py.downcast::<PyList>()?;
        let mut columns_original = Vec::<String>::with_capacity(columns_original_list.len());
        let mut columns_sql = Vec::<String>::with_capacity(columns_sql_list.len());
        for value in columns_original_list.iter() {
            columns_original.push(value.extract::<String>()?);
        }
        for value in columns_sql_list.iter() {
            columns_sql.push(value.extract::<String>()?);
        }
        if columns_original.is_empty() || columns_original.len() != columns_sql.len() {
            return Err(PyRuntimeError::new_err(format!(
                "invalid Rust MySQL loader column mapping for table {table_sql}"
            )));
        }
        tables.push(RustMysqlTable {
            path,
            table_sql,
            columns_original,
            columns_sql,
        });
    }
    Ok(tables)
}

fn load_parquet_tables_with_queryable<Q: Queryable>(
    table_items: &[RustMysqlTable],
    batch_size: usize,
    queryable: &mut Q,
) -> PyResult<RustMysqlLoadStats> {
    let mut stats = RustMysqlLoadStats::new();

    for item in table_items.iter() {
        let path = item.path.clone();
        let table_sql = item.table_sql.clone();
        let columns_original = &item.columns_original;
        let columns_sql = &item.columns_sql;

        let file = fs::File::open(&path)
            .map_err(|e| rust_mysql_context_error(&table_sql, &path, "opening parquet", e))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| {
                rust_mysql_context_error(&table_sql, &path, "reading parquet metadata", e)
            })?
            .with_batch_size(batch_size)
            .build()
            .map_err(|e| {
                rust_mysql_context_error(&table_sql, &path, "building parquet reader", e)
            })?;

        let placeholders = vec!["?"; columns_sql.len()].join(", ");
        let column_sql = columns_sql
            .iter()
            .map(|c| sql_quote_ident(c))
            .collect::<Vec<String>>()
            .join(", ");
        let insert_sql = format!(
            "INSERT INTO {} ({column_sql}) VALUES ({placeholders})",
            sql_quote_ident(&table_sql)
        );
        let mut table_rows = 0usize;
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                rust_mysql_context_error(&table_sql, &path, "reading parquet batch", e)
            })?;
            let mut arrays = Vec::<MysqlArrayView<'_>>::with_capacity(columns_original.len());
            let schema = batch.schema();
            for col in columns_original.iter() {
                let idx = schema.index_of(col).map_err(|e| {
                    rust_mysql_context_error(
                        &table_sql,
                        &path,
                        &format!("resolving parquet column {col}"),
                        e,
                    )
                })?;
                arrays.push(
                    MysqlArrayView::from_array(batch.column(idx), col).map_err(|e| {
                        rust_mysql_context_error(
                            &table_sql,
                            &path,
                            &format!("preparing parquet column {col}"),
                            e,
                        )
                    })?,
                );
            }

            let mut params_batch = Vec::<mysql::Params>::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let mut values = Vec::<mysql::Value>::with_capacity(arrays.len());
                for array in arrays.iter() {
                    values.push(array.mysql_value(row));
                }
                params_batch.push(mysql::Params::Positional(values));
            }
            if !params_batch.is_empty() {
                queryable
                    .exec_batch(&insert_sql, params_batch)
                    .map_err(|e| {
                        rust_mysql_context_error(
                            &table_sql,
                            &path,
                            &format!("inserting {} rows", batch.num_rows()),
                            e,
                        )
                    })?;
                stats.rows_loaded += batch.num_rows();
                table_rows += batch.num_rows();
            }
        }
        stats.files_loaded += 1;
        stats.tables_loaded += 1;
        stats.table_meta.push(RustMysqlTableMeta {
            table_sql,
            path: path.to_string_lossy().to_string(),
            rows_loaded: table_rows,
        });
    }

    Ok(stats)
}

#[allow(clippy::useless_conversion)]
#[pyfunction]
fn load_parquet_files_to_mysql(
    py: Python<'_>,
    tables: &Bound<'_, PyAny>,
    options: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let table_items = tables.downcast::<PyList>()?;
    let table_items = parse_rust_mysql_tables(table_items)?;
    let db_config = options
        .get_item("db_config")?
        .ok_or_else(|| PyRuntimeError::new_err("missing db_config for Rust MySQL loader"))?;
    let db_config = db_config.downcast::<PyDict>()?;
    let host = get_required_string_option(db_config, "host")?;
    let user = get_required_string_option(db_config, "user")?;
    let password = get_string_option(db_config, "password", "")?;
    let database = get_required_string_option(db_config, "database")?;
    let port = get_usize_option(db_config, "port", 3306)? as u16;
    let batch_size = get_usize_option(options, "batch_size", 1000)?.max(1);
    let connect_timeout_s = get_usize_option(options, "connect_timeout_s", 3)? as u64;
    let use_transaction = get_bool_option(options, "transaction", true)?;

    let opts = mysql::OptsBuilder::new()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(password))
        .db_name(Some(database))
        .tcp_connect_timeout(Some(std::time::Duration::from_secs(connect_timeout_s)));
    let (stats, load_ms) = py.allow_threads(move || -> PyResult<(RustMysqlLoadStats, u64)> {
        let pool = mysql::Pool::new(opts).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let mut conn = pool
            .get_conn()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let t0 = Instant::now();
        let stats = if use_transaction {
            let mut tx = conn
                .start_transaction(mysql::TxOpts::default())
                .map_err(|e| {
                    PyRuntimeError::new_err(format!(
                        "failed to start Rust MySQL loader transaction: {e}"
                    ))
                })?;
            match load_parquet_tables_with_queryable(&table_items, batch_size, &mut tx) {
                Ok(stats) => {
                    tx.commit().map_err(|e| {
                        PyRuntimeError::new_err(format!(
                            "failed to commit Rust MySQL loader transaction: {e}"
                        ))
                    })?;
                    stats
                }
                Err(err) => {
                    let rollback_result = tx.rollback();
                    if let Err(rollback_err) = rollback_result {
                        return Err(PyRuntimeError::new_err(format!(
                        "{}; additionally failed to rollback Rust MySQL loader transaction: {rollback_err}",
                        err
                    )));
                    }
                    return Err(err);
                }
            }
        } else {
            load_parquet_tables_with_queryable(&table_items, batch_size, &mut conn)?
        };
        Ok((stats, t0.elapsed().as_millis() as u64))
    })?;

    let result = PyDict::new_bound(py);
    result.set_item("ok", true)?;
    result.set_item("files_loaded", stats.files_loaded)?;
    result.set_item("tables_loaded", stats.tables_loaded)?;
    result.set_item("rows_loaded", stats.rows_loaded)?;
    let table_meta = PyList::empty_bound(py);
    for meta in stats.table_meta {
        let item = PyDict::new_bound(py);
        item.set_item("table_sql", meta.table_sql)?;
        item.set_item("path", meta.path)?;
        item.set_item("rows_loaded", meta.rows_loaded)?;
        table_meta.append(item)?;
    }
    result.set_item("tables", table_meta)?;
    result.set_item("transaction", use_transaction)?;
    let timings = PyDict::new_bound(py);
    timings.set_item("db.rust_mysql.load", load_ms)?;
    result.set_item("timings_ms", timings)?;
    Ok(result.into())
}

struct ColumnarChunkOut {
    tables: ColumnarTables,
    records_ok: usize,
}

fn flatten_columnar_chunk(chunk: &[(usize, Value)], options: &Options) -> ColumnarChunkOut {
    let mut tables = ColumnarTables::new();
    let mut records_ok = 0usize;
    for (local_i, record) in chunk {
        let global_i = options.index_offset + *local_i;
        let ctx = options.record_contexts.get(*local_i);
        flatten_record_columnar(record, global_i, ctx, options, &mut tables);
        records_ok += 1;
    }
    ColumnarChunkOut { tables, records_ok }
}

fn persist_indexed_json_values_columnar_inner(
    options: Options,
    parquet_root: PathBuf,
    indexed_values: IndexedJsonValues,
    extras: PersistExtras,
    total_start: Instant,
) -> PyResult<PersistOutput> {
    let PersistExtras {
        initial_records_failed,
        errors,
        error_indices,
        mut timings_ms,
    } = extras;

    let flatten_start = Instant::now();
    let mut tables = ColumnarTables::new();
    let records_ok = if options.parallel_workers >= 2 && indexed_values.len() >= 2 {
        let pool = rayon_pool(options.parallel_workers)?;
        let chunk_len = indexed_values
            .len()
            .div_ceil(options.parallel_workers)
            .max(1);
        let chunks: Vec<ColumnarChunkOut> = pool.install(|| {
            indexed_values
                .par_chunks(chunk_len)
                .map(|chunk| flatten_columnar_chunk(chunk, &options))
                .collect()
        });
        let mut count = 0usize;
        for chunk in chunks {
            count += chunk.records_ok;
            tables.merge_from(chunk.tables);
        }
        count
    } else {
        let chunk = flatten_columnar_chunk(&indexed_values, &options);
        let count = chunk.records_ok;
        tables.merge_from(chunk.tables);
        count
    };
    let flatten_ms = flatten_start.elapsed().as_millis() as u64;

    let parquet_start = Instant::now();
    let mut table_meta = Vec::<TableMeta>::new();
    let mut file_count = 0usize;
    let mut row_count = 0usize;
    let table_entries: Vec<(&String, &TableAccumulator)> = tables
        .tables
        .iter()
        .filter(|(_, table)| table.rows > 0)
        .collect();
    if options.parallel_table_writes && options.parallel_workers >= 2 && table_entries.len() >= 2 {
        let pool = rayon_pool(options.parallel_workers)?;
        let results: Vec<PyResult<TableMeta>> = pool.install(|| {
            table_entries
                .par_iter()
                .map(|(table, accumulator)| {
                    let (path, columns, rows_written) = write_columnar_table(
                        &parquet_root,
                        table,
                        accumulator,
                        options.batch_idx,
                        &options.index_key,
                    )?;
                    Ok(TableMeta {
                        table: (*table).clone(),
                        path: path.to_string_lossy().to_string(),
                        columns,
                        rows: rows_written,
                    })
                })
                .collect()
        });
        for result in results {
            let meta = result?;
            file_count += 1;
            row_count += meta.rows;
            table_meta.push(meta);
        }
    } else {
        for (table, accumulator) in table_entries {
            let (path, columns, rows_written) = write_columnar_table(
                &parquet_root,
                table,
                accumulator,
                options.batch_idx,
                &options.index_key,
            )?;
            file_count += 1;
            row_count += rows_written;
            table_meta.push(TableMeta {
                table: table.clone(),
                path: path.to_string_lossy().to_string(),
                columns,
                rows: rows_written,
            });
        }
    }
    let parquet_ms = parquet_start.elapsed().as_millis() as u64;
    timings_ms.push(("json.flatten", flatten_ms));
    timings_ms.push(("json.parquet.persist", parquet_ms));
    timings_ms.push(("rust_arrow.total", total_start.elapsed().as_millis() as u64));
    let records_failed = initial_records_failed;
    let records_read = records_ok + records_failed;

    Ok(PersistOutput {
        records_read,
        bytes_read: 0,
        records_ok,
        records_failed,
        parquet_files_persisted: file_count,
        parquet_rows_emitted: row_count,
        parquet_batches_total: usize::from(file_count > 0),
        tables: table_meta,
        errors,
        error_indices,
        error_records: Vec::new(),
        timings_ms,
        id_compaction_state: None,
    })
}

fn persist_indexed_json_values_inner(
    options: Options,
    parquet_root: PathBuf,
    indexed_values: IndexedJsonValues,
    extras: PersistExtras,
    total_start: Instant,
) -> PyResult<PersistOutput> {
    if options.columnar_accumulator && !options.id_compaction.enabled {
        return persist_indexed_json_values_columnar_inner(
            options,
            parquet_root,
            indexed_values,
            extras,
            total_start,
        );
    }

    let PersistExtras {
        initial_records_failed,
        mut errors,
        error_indices,
        mut timings_ms,
    } = extras;
    let flatten_start = Instant::now();
    let outs: Vec<RecordOut> = if options.parallel_workers >= 2 && indexed_values.len() >= 2 {
        let pool = rayon_pool(options.parallel_workers)?;
        pool.install(|| {
            indexed_values
                .par_iter()
                .map(|(local_i, record)| {
                    let global_i = options.index_offset + *local_i;
                    let ctx = options.record_contexts.get(*local_i);
                    flatten_record(record, global_i, ctx, &options)
                })
                .collect()
        })
    } else {
        indexed_values
            .iter()
            .map(|(local_i, record)| {
                let global_i = options.index_offset + *local_i;
                let ctx = options.record_contexts.get(*local_i);
                flatten_record(record, global_i, ctx, &options)
            })
            .collect()
    };
    let flatten_ms = flatten_start.elapsed().as_millis() as u64;

    let mut tables: TableRows = BTreeMap::new();
    let mut records_ok = 0usize;
    let mut records_failed = initial_records_failed;
    for out in outs {
        if out.ok {
            records_ok += 1;
            tables
                .entry(options.base_table.clone())
                .or_default()
                .push(out.main);
            for (sub_key, rows) in out.subs {
                let table = table_for_sub(&options, &sub_key);
                tables.entry(table).or_default().extend(rows);
            }
            for (table, rows) in out.excepted {
                tables.entry(table).or_default().extend(rows);
            }
        } else {
            records_failed += 1;
            if let Some(err) = out.error {
                errors.push(err);
            }
        }
    }

    let parquet_start = Instant::now();
    let mut id_compaction_state = IdCompactionState::default();
    let mut table_meta = Vec::<TableMeta>::new();
    let mut file_count = 0usize;
    let mut row_count = 0usize;
    let table_entries: Vec<(&String, &Vec<Row>)> =
        tables.iter().filter(|(_, rows)| !rows.is_empty()).collect();
    if options.id_compaction.enabled {
        for (table, rows) in table_entries {
            let compacted_rows = rows
                .iter()
                .map(|row| id_compaction_state.compact_row(&options, table, row))
                .collect::<PyResult<Vec<Row>>>()?;
            let (path, columns, rows_written) = write_table(
                &parquet_root,
                table,
                &compacted_rows,
                options.batch_idx,
                &options.index_key,
            )?;
            file_count += 1;
            row_count += rows_written;
            table_meta.push(TableMeta {
                table: table.clone(),
                path: path.to_string_lossy().to_string(),
                columns,
                rows: rows_written,
            });
        }
    } else if options.parallel_table_writes
        && options.parallel_workers >= 2
        && table_entries.len() >= 2
    {
        let pool = rayon_pool(options.parallel_workers)?;
        let results: Vec<PyResult<TableMeta>> = pool.install(|| {
            table_entries
                .par_iter()
                .map(|(table, rows)| {
                    let (path, columns, rows_written) = write_table(
                        &parquet_root,
                        table,
                        rows,
                        options.batch_idx,
                        &options.index_key,
                    )?;
                    Ok(TableMeta {
                        table: (*table).clone(),
                        path: path.to_string_lossy().to_string(),
                        columns,
                        rows: rows_written,
                    })
                })
                .collect()
        });
        for result in results {
            let meta = result?;
            file_count += 1;
            row_count += meta.rows;
            table_meta.push(meta);
        }
    } else {
        for (table, rows) in table_entries {
            let (path, columns, rows_written) = write_table(
                &parquet_root,
                table,
                rows,
                options.batch_idx,
                &options.index_key,
            )?;
            file_count += 1;
            row_count += rows_written;
            table_meta.push(TableMeta {
                table: table.clone(),
                path: path.to_string_lossy().to_string(),
                columns,
                rows: rows_written,
            });
        }
    }
    let parquet_ms = parquet_start.elapsed().as_millis() as u64;
    timings_ms.push(("json.flatten", flatten_ms));
    timings_ms.push(("json.parquet.persist", parquet_ms));
    timings_ms.push(("rust_arrow.total", total_start.elapsed().as_millis() as u64));
    let records_read = records_ok + records_failed;

    Ok(PersistOutput {
        records_read,
        bytes_read: 0,
        records_ok,
        records_failed,
        parquet_files_persisted: file_count,
        parquet_rows_emitted: row_count,
        parquet_batches_total: usize::from(file_count > 0),
        tables: table_meta,
        errors,
        error_indices,
        error_records: Vec::new(),
        timings_ms,
        id_compaction_state: if options.id_compaction.enabled {
            Some(id_compaction_state)
        } else {
            None
        },
    })
}

fn persist_output_to_py(
    py: Python<'_>,
    output: PersistOutput,
    id_compaction_options: &IdCompactionOptions,
) -> PyResult<PyObject> {
    let result = PyDict::new_bound(py);
    result.set_item("ok", true)?;
    result.set_item("effective_backend", "rust-arrow")?;
    result.set_item("records_read", output.records_read)?;
    result.set_item("bytes_read", output.bytes_read)?;
    result.set_item("records_ok", output.records_ok)?;
    result.set_item("records_failed", output.records_failed)?;
    result.set_item("parquet_files_persisted", output.parquet_files_persisted)?;
    result.set_item("parquet_rows_emitted", output.parquet_rows_emitted)?;
    result.set_item("parquet_tables_written", output.tables.len())?;
    result.set_item("parquet_batches_total", output.parquet_batches_total)?;
    let table_items = PyList::empty_bound(py);
    for meta in output.tables {
        let item = PyDict::new_bound(py);
        item.set_item("table", meta.table)?;
        item.set_item("path", meta.path)?;
        item.set_item("columns", meta.columns)?;
        item.set_item("rows", meta.rows)?;
        table_items.append(item)?;
    }
    result.set_item("tables", table_items)?;
    result.set_item("errors", output.errors)?;
    result.set_item("error_indices", output.error_indices)?;
    let error_records = PyList::empty_bound(py);
    for error_record in output.error_records {
        let item = PyDict::new_bound(py);
        item.set_item("source_path", error_record.source_path)?;
        item.set_item("line_no", error_record.line_no)?;
        item.set_item("record_index", error_record.record_index)?;
        item.set_item("raw_line", error_record.raw_line)?;
        item.set_item("error", error_record.error)?;
        error_records.append(item)?;
    }
    result.set_item("error_records", error_records)?;
    let timings = PyDict::new_bound(py);
    for (key, value) in output.timings_ms {
        timings.set_item(key, value)?;
    }
    result.set_item("timings_ms", timings)?;
    if let Some(state) = output.id_compaction_state {
        result.set_item("id_compaction", state.to_py(py, id_compaction_options)?)?;
    }
    Ok(result.into())
}

#[allow(clippy::useless_conversion)]
fn persist_indexed_json_values(
    py: Python<'_>,
    options: Options,
    parquet_root: PathBuf,
    indexed_values: IndexedJsonValues,
    extras: PersistExtras,
    total_start: Instant,
) -> PyResult<PyObject> {
    let id_compaction_options = options.id_compaction.clone();
    let output = py.allow_threads(move || {
        persist_indexed_json_values_inner(
            options,
            parquet_root,
            indexed_values,
            extras,
            total_start,
        )
    })?;
    persist_output_to_py(py, output, &id_compaction_options)
}

fn jsonl_context(source_path: &str, line_no: usize, record_index: usize) -> Value {
    let mut ctx = serde_json::Map::new();
    ctx.insert(
        "source_path".to_string(),
        Value::String(source_path.to_string()),
    );
    ctx.insert("line_no".to_string(), Value::Number(Number::from(line_no)));
    ctx.insert(
        "record_index".to_string(),
        Value::Number(Number::from(record_index)),
    );
    Value::Object(ctx)
}

#[allow(clippy::too_many_arguments)]
fn push_jsonl_error(
    errors: &mut Vec<String>,
    error_indices: &mut Vec<usize>,
    error_records: &mut Vec<JsonlErrorRecord>,
    source_path: &str,
    line_no: usize,
    record_index: usize,
    raw_line: &[u8],
    error: String,
) {
    errors.push(format!("record {record_index}: {error}"));
    error_indices.push(record_index);
    error_records.push(JsonlErrorRecord {
        source_path: source_path.to_string(),
        line_no,
        record_index,
        raw_line: String::from_utf8_lossy(trim_ascii_whitespace(raw_line)).to_string(),
        error,
    });
}

fn merge_persist_outputs(target: &mut PersistOutput, output: PersistOutput) {
    target.records_ok += output.records_ok;
    target.records_failed += output.records_failed;
    target.parquet_files_persisted += output.parquet_files_persisted;
    target.parquet_rows_emitted += output.parquet_rows_emitted;
    target.parquet_batches_total += output.parquet_batches_total;
    target.tables.extend(output.tables);
    target.errors.extend(output.errors);
    target.error_indices.extend(output.error_indices);
    target.error_records.extend(output.error_records);
    for (key, value) in output.timings_ms {
        if matches!(key, "json.flatten" | "json.parquet.persist") {
            if let Some((_, existing)) = target.timings_ms.iter_mut().find(|(name, _)| *name == key)
            {
                *existing += value;
            } else {
                target.timings_ms.push((key, value));
            }
        }
    }
}

fn persist_jsonl_sources_inner(
    options: Options,
    parquet_root: PathBuf,
    sources: Vec<String>,
    chunk_size: usize,
    max_records: Option<usize>,
    total_start: Instant,
) -> PyResult<PersistOutput> {
    if options.id_compaction.enabled {
        return Err(PyRuntimeError::new_err(
            "rust-arrow direct JSONL file parser does not support id_compaction yet; use batch raw JSONL parsing",
        ));
    }

    let mut aggregate = PersistOutput {
        records_read: 0,
        bytes_read: 0,
        records_ok: 0,
        records_failed: 0,
        parquet_files_persisted: 0,
        parquet_rows_emitted: 0,
        parquet_batches_total: 0,
        tables: Vec::new(),
        errors: Vec::new(),
        error_indices: Vec::new(),
        error_records: Vec::new(),
        timings_ms: vec![
            ("rust_arrow.json_parse", 0),
            ("json.flatten", 0),
            ("json.parquet.persist", 0),
        ],
        id_compaction_state: None,
    };

    let collect_contexts = !options.except_keys.is_empty();
    let mut values = IndexedJsonValues::with_capacity(chunk_size);
    let mut contexts = if collect_contexts {
        Vec::<Value>::with_capacity(chunk_size)
    } else {
        Vec::new()
    };
    let mut chunk_start_index = options.index_offset;
    let mut next_record_index = options.index_offset;
    let mut batch_idx = options.batch_idx;
    let mut parse_ns = 0u128;

    let flush_chunk = |values: &mut IndexedJsonValues,
                       contexts: &mut Vec<Value>,
                       batch_idx: &mut usize,
                       chunk_start_index: usize,
                       aggregate: &mut PersistOutput,
                       options: &Options|
     -> PyResult<()> {
        if values.is_empty() {
            return Ok(());
        }
        let mut chunk_options = options.clone();
        chunk_options.batch_idx = *batch_idx;
        chunk_options.index_offset = chunk_start_index;
        chunk_options.record_contexts = if collect_contexts {
            std::mem::replace(contexts, Vec::with_capacity(chunk_size))
        } else {
            Vec::new()
        };
        let chunk_values = std::mem::replace(values, Vec::with_capacity(chunk_size));
        let output = persist_indexed_json_values_inner(
            chunk_options,
            parquet_root.clone(),
            chunk_values,
            PersistExtras {
                initial_records_failed: 0,
                errors: Vec::new(),
                error_indices: Vec::new(),
                timings_ms: Vec::new(),
            },
            Instant::now(),
        )?;
        merge_persist_outputs(aggregate, output);
        *batch_idx += 1;
        Ok(())
    };

    'sources: for source in sources.iter() {
        let path = PathBuf::from(source);
        let file = fs::File::open(&path).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "failed to open Rust JSONL source {}: {e}",
                path.display()
            ))
        })?;
        let mut reader = BufReader::new(file);
        let mut raw_line = Vec::<u8>::new();
        let mut line_no = 0usize;
        loop {
            raw_line.clear();
            let bytes = reader.read_until(b'\n', &mut raw_line).map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "failed to read Rust JSONL source {}: {e}",
                    path.display()
                ))
            })?;
            if bytes == 0 {
                break;
            }
            line_no += 1;
            if trim_ascii_whitespace(&raw_line).is_empty() {
                continue;
            }
            if max_records.is_some_and(|limit| aggregate.records_read >= limit) {
                break 'sources;
            }
            aggregate.records_read += 1;
            aggregate.bytes_read += raw_line.len();
            let record_index = next_record_index;
            next_record_index += 1;

            let parse_start = Instant::now();
            let parsed = parse_json_bytes_value(&raw_line);
            parse_ns += parse_start.elapsed().as_nanos();

            match parsed {
                Ok(Some(value @ Value::Object(_))) => match validate_json_numbers(&value) {
                    Ok(()) => {
                        if values.is_empty() {
                            chunk_start_index = record_index;
                        }
                        let local_index = record_index - chunk_start_index;
                        values.push((local_index, value));
                        if collect_contexts {
                            while contexts.len() < local_index {
                                contexts.push(Value::Null);
                            }
                            contexts.push(jsonl_context(source, line_no, record_index));
                        }
                    }
                    Err(e) => {
                        aggregate.records_failed += 1;
                        push_jsonl_error(
                            &mut aggregate.errors,
                            &mut aggregate.error_indices,
                            &mut aggregate.error_records,
                            source,
                            line_no,
                            record_index,
                            &raw_line,
                            format!("{e}; use the Python backend"),
                        );
                    }
                },
                Ok(Some(value)) => {
                    aggregate.records_failed += 1;
                    push_jsonl_error(
                        &mut aggregate.errors,
                        &mut aggregate.error_indices,
                        &mut aggregate.error_records,
                        source,
                        line_no,
                        record_index,
                        &raw_line,
                        format!(
                            "non-dict JSON record encountered (type={})",
                            value_type_name(&value)
                        ),
                    );
                }
                Ok(None) => {}
                Err(e) => {
                    aggregate.records_failed += 1;
                    push_jsonl_error(
                        &mut aggregate.errors,
                        &mut aggregate.error_indices,
                        &mut aggregate.error_records,
                        source,
                        line_no,
                        record_index,
                        &raw_line,
                        e,
                    );
                }
            }

            if values.len() >= chunk_size {
                flush_chunk(
                    &mut values,
                    &mut contexts,
                    &mut batch_idx,
                    chunk_start_index,
                    &mut aggregate,
                    &options,
                )?;
            }
        }
    }

    flush_chunk(
        &mut values,
        &mut contexts,
        &mut batch_idx,
        chunk_start_index,
        &mut aggregate,
        &options,
    )?;
    if let Some((_, total_ms)) = aggregate
        .timings_ms
        .iter_mut()
        .find(|(name, _)| *name == "rust_arrow.total")
    {
        *total_ms = total_start.elapsed().as_millis() as u64;
    } else {
        aggregate
            .timings_ms
            .push(("rust_arrow.total", total_start.elapsed().as_millis() as u64));
    }
    if let Some((_, parse_ms)) = aggregate
        .timings_ms
        .iter_mut()
        .find(|(name, _)| *name == "rust_arrow.json_parse")
    {
        *parse_ms = (parse_ns / 1_000_000) as u64;
    }
    Ok(aggregate)
}

#[allow(clippy::useless_conversion)]
#[pyfunction]
fn persist_json_batch(
    py: Python<'_>,
    records: &Bound<'_, PyAny>,
    options: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let total_start = Instant::now();
    let options = parse_options(options)?;
    let parquet_root = prepare_parquet_root(&options.parquet_dir)?;
    let py_to_json_start = Instant::now();
    let list = records.downcast::<PyList>()?;
    let mut values = Vec::with_capacity(list.len());
    for (i, item) in list.iter().enumerate() {
        values.push((i, py_to_json(&item)?));
    }
    let py_to_json_ms = py_to_json_start.elapsed().as_millis() as u64;

    persist_indexed_json_values(
        py,
        options,
        parquet_root,
        values,
        PersistExtras {
            initial_records_failed: 0,
            errors: Vec::new(),
            error_indices: Vec::new(),
            timings_ms: vec![("rust_arrow.py_to_json", py_to_json_ms)],
        },
        total_start,
    )
}

#[allow(clippy::useless_conversion)]
#[pyfunction]
fn persist_json_lines_batch(
    py: Python<'_>,
    lines: &Bound<'_, PyAny>,
    options: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let total_start = Instant::now();
    let options = parse_options(options)?;
    let parquet_root = prepare_parquet_root(&options.parquet_dir)?;
    let parse_start = Instant::now();
    let list = lines.downcast::<PyList>()?;
    let mut values = Vec::with_capacity(list.len());
    let mut records_failed = 0usize;
    let mut errors = Vec::<String>::new();
    let mut error_indices = Vec::<usize>::new();
    for (i, item) in list.iter().enumerate() {
        match parse_json_line_value(&item)? {
            Ok(Some(value @ Value::Object(_))) => match validate_json_numbers(&value) {
                Ok(()) => values.push((i, value)),
                Err(e) => {
                    records_failed += 1;
                    error_indices.push(i);
                    errors.push(format!("record {i}: {e}; use the Python backend"));
                }
            },
            Ok(Some(value)) => {
                records_failed += 1;
                error_indices.push(i);
                errors.push(format!(
                    "record {i}: non-dict JSON record encountered (type={})",
                    value_type_name(&value)
                ));
            }
            Ok(None) => {}
            Err(e) => {
                records_failed += 1;
                error_indices.push(i);
                errors.push(format!("record {i}: {e}"));
            }
        }
    }
    let parse_ms = parse_start.elapsed().as_millis() as u64;

    persist_indexed_json_values(
        py,
        options,
        parquet_root,
        values,
        PersistExtras {
            initial_records_failed: records_failed,
            errors,
            error_indices,
            timings_ms: vec![("rust_arrow.json_parse", parse_ms)],
        },
        total_start,
    )
}

#[allow(clippy::useless_conversion)]
#[pyfunction]
fn persist_jsonl_sources(
    py: Python<'_>,
    sources: &Bound<'_, PyAny>,
    options: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let total_start = Instant::now();
    let chunk_size = get_usize_option(options, "chunk_size", 1000)?.max(1);
    let max_records = match options.get_item("max_records")? {
        Some(value) if !value.is_none() => Some(value.extract::<usize>()?),
        _ => None,
    };
    let options = parse_options(options)?;
    let id_compaction_options = options.id_compaction.clone();
    let parquet_root = prepare_parquet_root(&options.parquet_dir)?;
    let source_items = sources.downcast::<PyList>()?;
    let mut source_paths = Vec::<String>::with_capacity(source_items.len());
    for item in source_items.iter() {
        source_paths.push(item.extract::<String>()?);
    }
    let output = py.allow_threads(move || {
        persist_jsonl_sources_inner(
            options,
            parquet_root,
            source_paths,
            chunk_size,
            max_records,
            total_start,
        )
    })?;
    persist_output_to_py(py, output, &id_compaction_options)
}

#[pymodule]
fn kisti_json_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(persist_json_batch, m)?)?;
    m.add_function(wrap_pyfunction!(persist_json_lines_batch, m)?)?;
    m.add_function(wrap_pyfunction!(persist_jsonl_sources, m)?)?;
    m.add_function(wrap_pyfunction!(load_parquet_files_to_mysql, m)?)?;
    Ok(())
}
