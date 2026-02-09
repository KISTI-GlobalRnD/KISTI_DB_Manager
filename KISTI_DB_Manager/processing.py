# KISTI_DB_Manager/processing.py
"""
Note
----
Made by Young Jin Kim (kimyoungjin06@gmail.com)
Last Update: 2024.03.17, YJ Kim

MariaDB/MySQL Handling for All type DB
To preprocess, import, export and manage the DB

# Updates
## 2024.03.17
- Add exception part for unstructured json branches
    - related: flatten_json_separate_lists, ..., flatten_json_separate_lists
    - except_keys: excepted part from unstructured json branches

Example
-------
"""
import numpy as np
import pandas as pd
from tqdm import tqdm


__all__ = ["conv_HTML_entities", "flatten_dict", "read_a_xml", "flatten_nested_json_with_list", 
           "read_dict_from_zip", "extract_rows_from_jsons", "extract_data_from_jsons", "read_dict_from_gz", "json_to_key_pairs",
          "key_pair_to_df", "excepted_regularization", "separate_excepted", "json_parsing"]

def conv_HTML_entities(content, replace_list=['p', 'sub', 'sup', 'i', 'b', ], 
                       rounds=[('<', '>'), ('</', '>')], 
                       trans=[('%_lt_;', '%_gt_;'), ('%_lt_;/', '%_gt_;')], verbose=False):
    import re
    content_conv = content[:]
    for r in tqdm(replace_list, desc='Convert HTML Entities: '):
        for i, ro in enumerate(rounds):
            _tag = f"{ro[0]}{r}{ro[1]}"
            _tag_to = f"{trans[i][0]}{r}{trans[i][1]}"
            content_conv = re.sub(_tag, _tag_to, content_conv)
            if verbose:
                res = re.findall(_tag, content)
                print(len(res), _tag, "converted")
    return content_conv


def flatten_dict(d):
    """
    Recursively flattens a nested dictionary so that nested keys are concatenated into
    a single key separated by dots.

    Parameters:
    d (dict): The dictionary to flatten.

    Returns:
    dict: A new dictionary with flattened keys.
    """
    def items():
        for key, value in d.items():
            if isinstance(value, dict):
                for subkey, subvalue in flatten_dict(value).items():
                    yield key + "." + subkey, subvalue
            else:
                yield key, value
    return dict(items())
    
def read_a_xml(f):
    """
    Parses an XML file or string and converts it into a flattened dictionary using xmltodict.

    Parameters:
    f (str or file): XML data as a string or a file object to parse.

    Returns:
    dict: A dictionary representation of the XML data, with nested structures flattened.
    """
    import xmltodict

    _x = xmltodict.parse(f)
    _dict = flatten_dict(_x)
    return _dict

def extract_key(_dict, key, key_on):
    """
    Extracts a key-value pair from a dictionary and prepares a DataFrame for the key
    if it contains list-like data. Handles both multiple values (list of dicts) and
    single value cases.

    Parameters:
    _dict (dict): The dictionary from which to extract the data.
    key (str): The key in the dictionary for the data to extract.
    key_on (str): The primary key or reference key to maintain relational integrity.

    Returns:
    tuple: A tuple containing the updated dictionary (with the key removed) and a 
    DataFrame constructed from the list associated with the key or an empty DataFrame 
    if the key does not exist or has no additional data.
    """
    
    ID_on = _dict[key_on]
    try: # Multiple-Values Case
        Lists = _dict.pop(key) # Exit from here
        cols = Lists[0].keys()
        old_cols = [key_on] + [f'{c}' for c in cols]
        new_cols = [key_on] + [f'{key}.{c}' for c in cols]
        res = []
        for subdict in Lists:
            subdict[key_on] = ID_on
            res.append(pd.DataFrame(subdict, index=[0]))
    
        _df = pd.concat(res).reset_index(drop=True)
        _df = _df[old_cols]
        _df.columns = new_cols
    except: # Single Value Case
        subdict = {}
        subdict[key_on] = ID_on
        __dict = _dict.copy()
        for k in __dict.keys():
            if key in k:
                v = _dict.pop(k)
                subdict[k] = v
        if len(subdict.keys()) > 1:
            _df = pd.DataFrame(subdict, index=[0])
        else:
            _df = pd.DataFrame([])
    return _dict, _df

def read_NSF_from_zip(zipf, path, key_on, ext_keys=None):
    """
    Reads XML files from a specified ZIP archive, parses each XML file into a flattened
    dictionary, and extracts specified keys into separate DataFrames. It aggregates all
    the XML files' data into a single DataFrame and handles extraction of additional
    specified keys into separate DataFrames.

    Parameters:
    zipf (str): The filename of the ZIP file within the path.
    path (str): The path to the directory containing the ZIP file.
    key_on (str): The primary key or reference key used to maintain relational integrity
    across the extracted DataFrames.
    ext_keys (list of str, optional): A list of keys to specifically extract into separate
    DataFrames from each XML file's data.

    Returns:
    tuple: A tuple containing the main DataFrame with aggregated data from all XML files,
    a list of DataFrames for each key specified in ext_keys, and a log list with entries
    for files that could not be processed.
    """
    
    if ext_keys is None:
        ext_keys = []

    data = []
    sub_datas = {}
    logs = []
    for ek in ext_keys:
        sub_datas[ek] = []
    
    import os
    import zipfile

    with zipfile.ZipFile(os.path.join(path, zipf), "r") as zipObj:
        listOfFileNames = zipObj.namelist()
        for fileName in tqdm(listOfFileNames):
            if fileName.endswith('xml'):
                try:
                    zipRead = zipObj.read(fileName)
                    _dict = read_a_xml(zipRead)
                    for ek in ext_keys:
                        _dict, sdata = extract_key(_dict, ek, key_on)
                        sub_datas[ek].append(sdata)
                    try:
                        curr_df = pd.DataFrame(_dict, index=[0])
                    except:
                        print(_dict)
                        raise "Check!"
                    data.append(curr_df)
                except:  # Error Log
                    logs.append([zipf, fileName])
    _df = pd.concat(data).reset_index(drop=True)
    df_subs = []
    for ek in ext_keys:
        df_subs.append(pd.concat(sub_datas[ek]).reset_index(drop=True))
    new_cols = [key_on] + sorted(list(set(_df.columns) - {key_on}))
    return _df[new_cols], df_subs, logs


# def find_keys_with_list(data, prefix=''):
#     """
#     Recursively finds and returns keys in a nested dictionary whose values are lists.

#     Parameters:
#     data (dict): The dictionary to search through.
#     prefix (str): The prefix for keys to maintain the path in nested dictionaries.

#     Returns:
#     list: A list of keys that have list values, with paths indicated for nested dictionaries.
#     """
#     keys_with_list = []

#     for key, value in data.items():
#         # Construct full key path for nested dictionaries
#         full_key = f"{prefix}.{key}" if prefix else key

#         if isinstance(value, list):
#             keys_with_list.append(full_key)
#         elif isinstance(value, dict):
#             # Recursively search for list-containing keys in nested dictionaries
#             keys_with_list.extend(find_keys_with_list(value, full_key))

#     return keys_with_list


# def extract_nested_with_list(data):
#     """
#     Recursively extracts and returns parts of a nested dictionary where at least one value is a list.

#     Parameters:
#     data (dict): The dictionary to search through.

#     Returns:
#     dict: A new dictionary including only the keys (and nested keys) where at least one value is a list.
#     """
#     result = {}

#     for key, value in data.items():
#         if isinstance(value, list):
#             # Directly include keys with list values
#             result[key] = value
#         elif isinstance(value, dict):
#             # Recursively process nested dictionaries
#             extracted = extract_nested_with_list(value)
#             if extracted:
#                 # Only include nested dictionaries that have at least one list value
#                 result[key] = extracted

#     return result

# def flatten_json_separate_lists(nested_json, except_keys=[], separator='__', parent=''):
#     """
#     Flattens a nested JSON object (Python dictionary) and separates the output into
#     two dictionaries: one for keys with single (non-list) values, and another for keys
#     with multiple values (lists), with keys indicating the path through the original
#     nested structure.

#     Parameters:
#     nested_json (dict): The JSON object (dictionary) to flatten.
#     separator (str): The separator to use for concatenating nested keys. Default is '.'.

#     Returns:
#     tuple: A tuple containing two dictionaries. The first dictionary contains flattened
#            keys with single (non-list) values. The second dictionary contains flattened
#            keys with list values.
#     """
#     single_values = {}
#     multiple_values = {}
#     excepted_values = {}

#     def flatten(x, name=''):
#         if isinstance(x, dict):
#             for a in x:
#                 if a in except_keys:
#                     excepted_values[a] = x[a]
#                 else:
#                     flatten(x[a], name + a + separator)
#         elif isinstance(x, list):
#             multiple_values[name[:-1*len(separator)]] = x
#             # print(name)
#         else:
#             single_values[name[:-1*len(separator)]] = x

#     flatten(nested_json)
#     print(single_values, '*****', multiple_values, '*****', excepted_values, '\n\n')
#     return single_values, multiple_values, excepted_values


def flatten_json_separate_lists(nested_json, except_keys=None, sep="__", parent=""):
    """
    Flattens a nested JSON object (Python dictionary) and separates the output into
    two dictionaries: one for keys with single (non-list) values, and another for keys
    with multiple values (lists), with keys indicating the path through the original
    nested structure.

    This version keeps lists as is but flattens any dictionaries within the lists
    and stores them in multiple_values.

    Parameters:
    nested_json (dict): The JSON object (dictionary) to flatten.
    separator (str): The separator to use for concatenating nested keys. Default is '__'.
    except_keys (list): Keys to exclude from flattening.

    Returns:
    tuple: A tuple containing two dictionaries. The first dictionary contains flattened
           keys with single (non-list) values. The second dictionary contains flattened
           keys with list values. The third dictionary contains excepted values.
    """
    if except_keys is None:
        except_keys = []
    # Normalize once for faster membership checks.
    except_set = {str(k) for k in except_keys if str(k)}

    single_values = {}
    multiple_values = {}
    excepted_values = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                full_key = f"{name}{a}" if name else str(a)
                if a in except_set or full_key in except_set:
                    excepted_values[full_key] = x[a]
                else:
                    flatten(x[a], full_key + sep)
        elif isinstance(x, list):
            # If the list contains dictionaries, flatten each dictionary but keep the list structure
            if len(x) > 0 and isinstance(x[0], dict):
                flat_list = []
                for i, item in enumerate(x):
                    # Recursively flatten each dictionary inside the list
                    sub_single, sub_multiple, sub_excepted = flatten_json_separate_lists(
                        item, except_keys=except_keys, sep=sep, parent=name)
                    # Append each flattened dictionary to the list
                    flat_list.append({**sub_single, **sub_multiple})
                multiple_values[name[:-len(sep)]] = flat_list
            else:
                # If it's a simple list, store it directly in multiple_values
                multiple_values[name[:-len(sep)]] = x
        else:
            single_values[name[:-len(sep)]] = x

    flatten(nested_json)
    return single_values, multiple_values, excepted_values


def multiples_to_dataframes(multiples, sep='__'):
    """
    Converts a dictionary containing keys mapped to list values into a dictionary
    of pandas DataFrames, where each list is converted into a DataFrame. Column names
    in each DataFrame are prefixed with the key and a separator.

    Parameters:
    multiples (dict): A dictionary with keys mapped to lists, typically representing
                      multiple values from a nested JSON structure.
    separation (str): A string used to separate the key prefix from the column names
                      in the DataFrame. Default is '.'.

    Returns:
    dict: A dictionary where each key is mapped to a DataFrame constructed from the
          list values associated with that key in the input dictionary, with column
          names prefixed by the key.
    """
    dataframes = {}

    for key, value_list in multiples.items():
        # Check if the list contains dictionaries (for structured DataFrame creation)
        if value_list and isinstance(value_list[0], dict):
            # Prefix column names with the key and separator
            df = pd.DataFrame(value_list)
            df.columns = [f'{key}{sep}{col}' if col != key else col for col in df.columns]
        else:
            # print(key)
            # If the list contains primitive types, create a single-column DataFrame with the prefixed column name
            df = pd.DataFrame(value_list, columns=[f'{key}'])
        
        dataframes[key] = df
    
    return dataframes


def add_index_column_to_dfs(dfs, index_column_name, index_value):
    """
    Adds an index column to each DataFrame in a dictionary of DataFrames and
    populates it with a given value.

    Parameters:
    dfs (dict): A dictionary where each key is mapped to a pandas DataFrame.
    index_column_name (str): The name of the new index column to add to each DataFrame.
    index_value (str or int): The value to populate in the new index column.

    Returns:
    dict: The updated dictionary of DataFrames with the new index column added.
    """
    updated_dfs = {}
    
    for key, df in dfs.items():
        # Add the index column with the given value to the DataFrame
        updated_df = df.assign(**{index_column_name: index_value})
        sorted_cols = [index_column_name] + list(df.columns)
        updated_dfs[key] = updated_df[sorted_cols]
    
    return updated_dfs


def flatten_nested_json_with_list(_json, index_key="id", index=0, except_keys=None, sep="__"):
    if except_keys is None:
        except_keys = []

    single, multiples, excepted = flatten_json_separate_lists(_json, except_keys=except_keys, sep=sep)
    if index_key not in single or single.get(index_key) in {None, ""}:
        single[index_key] = index
    _df = pd.DataFrame(single, index=[index])
    _id = _df.loc[index, index_key]
    df_subs = multiples_to_dataframes(multiples, sep=sep)
    df_subs_add_idx = add_index_column_to_dfs(df_subs, index_key, _id)

    for key, value in list(excepted.items()):
        if isinstance(value, dict):
            value[index_key] = _id
            excepted[key] = value
        else:
            excepted[key] = {index_key: _id, "value": value}
    return _df, df_subs_add_idx, excepted


def flatten_nested_json_with_list_rows(_json, index_key="id", index=0, except_keys=None, sep="__", colname_cache=None):
    """
    Row-oriented variant of flatten_nested_json_with_list for performance.

    Returns:
      - single_row: dict
      - sub_rows: dict[sub_key, list[dict]]
      - excepted: dict[branch, dict]
    """
    if except_keys is None:
        except_keys = []

    # Allow callers (extract_rows_from_jsons) to pass a pre-built set to avoid per-record conversion.
    if isinstance(except_keys, set):
        except_set = except_keys
    else:
        except_set = {str(k) for k in except_keys if str(k)}

    # Column name cache for subtable rows:
    #   {sep: {list_key: {item_col: full_col}}}
    if colname_cache is None:
        colname_cache = {}
    colname_cache_sep = colname_cache.setdefault(str(sep), {})

    if except_set:
        def _flatten_dict_keep_lists(obj) -> dict:
            out: dict = {}
            stack: list[tuple[object, str]] = [(obj, "")]
            stack_pop = stack.pop
            stack_append = stack.append
            sep_local = sep
            sep_len = len(sep_local)
            while stack:
                cur, prefix = stack_pop()
                if type(cur) is dict or isinstance(cur, dict):
                    for k, v in cur.items():
                        ks = k if type(k) is str else str(k)
                        full_key = (prefix + ks) if prefix else ks
                        # Preserve previous semantics: excepted keys inside list-items are dropped.
                        if ks in except_set or full_key in except_set:
                            continue
                        tv = type(v)
                        if tv is dict:
                            stack_append((v, full_key + sep_local))
                            continue
                        if tv is list:
                            if v and (type(v[0]) is dict or isinstance(v[0], dict)):
                                flat_list: list[dict] = []
                                flat_list_append = flat_list.append
                                for it in v:
                                    if type(it) is dict:
                                        tmp: dict = {}
                                        deep = False
                                        for kk, vv in it.items():
                                            tvv = type(vv)
                                            if tvv is dict or (
                                                tvv is list and vv and (type(vv[0]) is dict or isinstance(vv[0], dict))
                                            ):
                                                deep = True
                                                break
                                            kks = kk if type(kk) is str else str(kk)
                                            if kks in except_set:
                                                continue
                                            tmp[kks] = vv
                                        if not deep:
                                            flat_list_append(tmp)
                                        else:
                                            flat_list_append(_flatten_dict_keep_lists(it))
                                    elif isinstance(it, dict):
                                        flat_list_append(_flatten_dict_keep_lists(it))
                                out[full_key] = flat_list
                            else:
                                out[full_key] = v
                            continue
                        out[full_key] = v
                    continue
                key = prefix[: -sep_len] if prefix.endswith(sep_local) else prefix
                if key:
                    out[key] = cur
            return out
    else:
        def _flatten_dict_keep_lists(obj) -> dict:
            # Same as above but without except-key checks (common case: no exclusions).
            out: dict = {}
            stack: list[tuple[object, str]] = [(obj, "")]
            stack_pop = stack.pop
            stack_append = stack.append
            sep_local = sep
            sep_len = len(sep_local)
            while stack:
                cur, prefix = stack_pop()
                if type(cur) is dict or isinstance(cur, dict):
                    for k, v in cur.items():
                        ks = k if type(k) is str else str(k)
                        full_key = (prefix + ks) if prefix else ks
                        tv = type(v)
                        if tv is dict:
                            stack_append((v, full_key + sep_local))
                            continue
                        if tv is list:
                            if v and (type(v[0]) is dict or isinstance(v[0], dict)):
                                flat_list: list[dict] = []
                                flat_list_append = flat_list.append
                                for it in v:
                                    if type(it) is dict:
                                        tmp: dict = {}
                                        deep = False
                                        for kk, vv in it.items():
                                            tvv = type(vv)
                                            if tvv is dict or (
                                                tvv is list and vv and (type(vv[0]) is dict or isinstance(vv[0], dict))
                                            ):
                                                deep = True
                                                break
                                            kks = kk if type(kk) is str else str(kk)
                                            tmp[kks] = vv
                                        if not deep:
                                            flat_list_append(tmp)
                                        else:
                                            flat_list_append(_flatten_dict_keep_lists(it))
                                    elif isinstance(it, dict):
                                        flat_list_append(_flatten_dict_keep_lists(it))
                                out[full_key] = flat_list
                            else:
                                out[full_key] = v
                            continue
                        out[full_key] = v
                    continue
                key = prefix[: -sep_len] if prefix.endswith(sep_local) else prefix
                if key:
                    out[key] = cur
            return out


    # Resolve record id early so we can build subtable rows on-the-fly without a second pass.
    # Keep semantics identical to the original flatten: if id is missing/blank or not a scalar
    # (dict/list), fall back to the record index.
    _id = index
    if isinstance(_json, dict) and index_key and (not except_set or str(index_key) not in except_set):
        try:
            cand = _json.get(index_key)
        except Exception:
            cand = None
        if cand not in {None, ""} and not isinstance(cand, (dict, list)):
            _id = cand

    single: dict = {}
    excepted: dict = {}
    sub_rows: dict[str, list[dict]] = {}

    def _process_value_list(key, value_list) -> None:
        if not value_list:
            return

        key_s = key if type(key) is str else str(key)
        rows: list[dict] = []
        first = value_list[0]
        if type(first) is dict or isinstance(first, dict):
            key_prefix = key_s + sep
            col_map = colname_cache_sep.get(key_s)
            if col_map is None:
                col_map = {}
                colname_cache_sep[key_s] = col_map
            rows_append = rows.append
            for item in value_list:
                if type(item) is dict:
                    pass
                elif isinstance(item, dict):
                    pass
                else:
                    continue

                # One-pass: build fast row while checking if we must fall back to deep flatten.
                row: dict = {index_key: _id}
                needs_deep_flatten = False
                for col, val in item.items():
                    tv = type(val)
                    if tv is dict or (tv is list and val and (type(val[0]) is dict or isinstance(val[0], dict))):
                        needs_deep_flatten = True
                        break
                    col_s = col if type(col) is str else str(col)
                    if except_set and col_s in except_set:
                        continue
                    try:
                        col2 = col_map[col_s]
                    except KeyError:
                        col2 = col_s if col_s == key_s else (key_prefix + col_s)
                        col_map[col_s] = col2
                    row[col2] = val

                if not needs_deep_flatten:
                    rows_append(row)
                    continue

                flat_item = _flatten_dict_keep_lists(item)
                row = {index_key: _id}
                for col, val in flat_item.items():
                    col_s = col if type(col) is str else str(col)
                    try:
                        col2 = col_map[col_s]
                    except KeyError:
                        col2 = col_s if col_s == key_s else (key_prefix + col_s)
                        col_map[col_s] = col2
                    row[col2] = val
                rows_append(row)
        else:
            for val in value_list:
                rows.append({index_key: _id, key_s: val})

        if rows:
            # key_s is always a stable string for table naming.
            sub_rows[key_s] = rows

    # Iterative traversal (avoids recursion + repeated dict merges).
    stack2: list[tuple[object, str]] = [(_json, "")]
    if except_set:
        while stack2:
            cur, prefix = stack2.pop()
            if type(cur) is dict:
                for k, v in cur.items():
                    ks = k if type(k) is str else str(k)
                    full_key = (prefix + ks) if prefix else ks
                    if ks in except_set or full_key in except_set:
                        excepted[full_key] = v
                        continue
                    tv = type(v)
                    if tv is dict:
                        stack2.append((v, full_key + sep))
                        continue
                    if tv is list:
                        _process_value_list(full_key, v)
                        continue
                    if v is None or tv is str or tv is int or tv is float or tv is bool:
                        single[full_key] = v
                        continue
                    single[full_key] = v
                continue

            # Non-dict record: store under `root` (best-effort).
            if prefix:
                key = prefix[: -len(sep)] if prefix.endswith(sep) else prefix
                single[key] = cur
            else:
                single["root"] = cur
    else:
        while stack2:
            cur, prefix = stack2.pop()
            if type(cur) is dict:
                for k, v in cur.items():
                    ks = k if type(k) is str else str(k)
                    full_key = (prefix + ks) if prefix else ks
                    tv = type(v)
                    if tv is dict:
                        stack2.append((v, full_key + sep))
                        continue
                    if tv is list:
                        _process_value_list(full_key, v)
                        continue
                    if v is None or tv is str or tv is int or tv is float or tv is bool:
                        single[full_key] = v
                        continue
                    single[full_key] = v
                continue

            # Non-dict record: store under `root` (best-effort).
            if prefix:
                key = prefix[: -len(sep)] if prefix.endswith(sep) else prefix
                single[key] = cur
            else:
                single["root"] = cur

    if index_key not in single or single.get(index_key) in {None, ""}:
        single[index_key] = _id

    return single, sub_rows, excepted


def _safe_flatten_nested_json_with_list_rows_worker(args):
    """
    ProcessPool worker wrapper around flatten_nested_json_with_list_rows.

    Returns a tuple so the caller can continue on per-record failures without the
    whole pool raising.
    """
    import traceback

    try:
        index, record, index_key, except_keys, sep = args
        row, sub_rows, excepted = flatten_nested_json_with_list_rows(
            record,
            index_key=index_key,
            index=index,
            except_keys=list(except_keys) if except_keys is not None else None,
            sep=sep,
        )
        return index, True, row, sub_rows, excepted, None
    except Exception as e:
        return (
            None,
            False,
            None,
            None,
            None,
            {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            },
        )


# ProcessPool worker config (set via initializer to reduce per-task IPC/pickling overhead).
_flatten_pool_index_key = None
_flatten_pool_except_set = None
_flatten_pool_sep = None
_flatten_pool_colname_cache = None


def _init_flatten_nested_json_with_list_rows_worker(index_key: str, except_keys, sep: str):
    global _flatten_pool_index_key, _flatten_pool_except_set, _flatten_pool_sep, _flatten_pool_colname_cache
    _flatten_pool_index_key = str(index_key) if index_key is not None else "id"
    _flatten_pool_except_set = set(except_keys or ())
    _flatten_pool_sep = str(sep) if sep is not None else "__"
    _flatten_pool_colname_cache = {}


def _safe_flatten_nested_json_with_list_rows_worker_v2(args):
    """ProcessPool worker wrapper using globals set by initializer."""
    import traceback

    try:
        index, record = args
        row, sub_rows, excepted = flatten_nested_json_with_list_rows(
            record,
            index_key=_flatten_pool_index_key or "id",
            index=index,
            except_keys=_flatten_pool_except_set,
            sep=_flatten_pool_sep or "__",
            colname_cache=_flatten_pool_colname_cache,
        )
        return index, True, row, sub_rows, excepted, None
    except Exception as e:
        return (
            None,
            False,
            None,
            None,
            None,
            {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            },
        )


def _safe_flatten_jsons_to_tsv_worker(args):
    """
    ProcessPool worker: flatten a chunk of JSON records and write TSV files per table.

    This avoids returning large Python objects (rows) to the parent process. The parent
    can then execute `LOAD DATA LOCAL INFILE` using the returned file paths.

    Returns (dict):
      - ok: bool
      - index_offset: int
      - records_ok / records_failed: int
      - errors: list[dict]  (best-effort per-record errors)
      - workdir: str (temporary directory containing TSV files)
      - main: fileinfo | None
      - subs: dict[sub_key, fileinfo]
      - excepted: dict[except_key, fileinfo]

    fileinfo:
      - path: str
      - columns: list[str] (canonical column names, file order)
      - rows: int
    """
    import os
    import tempfile
    import time
    import traceback
    import uuid

    try:
        # Backward-compatible args parsing:
        #   legacy: (index_offset, jsons, index_key, except_keys, sep, tmp_dir, record_contexts)
        #   new:    (..., base_table, extra_column_name, allowed_cols_by_table)
        if not isinstance(args, (list, tuple)) or len(args) < 7:
            raise TypeError("flatten_jsons_to_tsv_worker expects a tuple/list of length >= 7")

        index_offset = args[0]
        jsons = args[1]
        index_key = args[2]
        except_keys = args[3]
        sep = args[4]
        tmp_dir = args[5]
        record_contexts = args[6]
        base_table = args[7] if len(args) > 7 else None
        extra_column_name = args[8] if len(args) > 8 else None
        allowed_cols_by_table = args[9] if len(args) > 9 else None

        try:
            index_offset = int(index_offset or 0)
        except Exception:
            index_offset = 0

        index_key = str(index_key) if index_key is not None else "id"
        sep = str(sep) if sep is not None else "__"
        tmp_dir = str(tmp_dir or "/tmp")
        base_table = str(base_table or "")

        extra_canon = None
        if extra_column_name:
            extra_canon = str(extra_column_name).replace(".", sep)

        allowed_map: dict[str, set[str]] | None = None
        if extra_canon and isinstance(allowed_cols_by_table, dict) and allowed_cols_by_table:
            out: dict[str, set[str]] = {}
            for tn, cols in allowed_cols_by_table.items():
                tns = str(tn or "").strip()
                if not tns or cols is None:
                    continue
                try:
                    it = cols if isinstance(cols, (list, tuple, set)) else list(cols)
                except Exception:
                    it = []
                keep: set[str] = set()
                for c in it:
                    cs = str(c or "").strip()
                    if not cs:
                        continue
                    keep.add(cs.replace(".", sep))
                if keep:
                    # Always allow id + extra on the worker side (even if caller forgot to include them).
                    keep.add(index_key)
                    keep.add(extra_canon)
                    out[tns] = keep
            allowed_map = out or None

        # Normalize once so comparisons against excepted keys are stable.
        if except_keys is None:
            except_keys = []
        except_keys = [str(k).strip() for k in except_keys if str(k).strip()]
        except_set = set(except_keys)

        def _json_dumps_best_effort(value):
            try:
                import orjson

                return orjson.dumps(value).decode("utf-8")
            except Exception:
                import json

                try:
                    return json.dumps(value, ensure_ascii=False)
                except Exception:
                    return json.dumps(str(value), ensure_ascii=False)

        def _build_excepted_row(path: str, value, row: dict, context) -> dict:
            out: dict = {}
            if isinstance(value, dict):
                out.update(value)
            else:
                out["value"] = value

            out[index_key] = row.get(index_key)
            out["__except_path__"] = str(path)
            out["__except_raw_type__"] = type(value).__name__
            out["__except_raw_json__"] = _json_dumps_best_effort(value)

            if isinstance(context, dict):
                source_path = context.get("source_path")
                if source_path is not None:
                    out["__source_path__"] = source_path
                source_member = context.get("source_member")
                if source_member is not None:
                    out["__source_member__"] = source_member
                line_no = context.get("line_no")
                if line_no is not None:
                    out["__line_no__"] = line_no
                record_index = context.get("record_index")
                if record_index is not None:
                    out["__record_index__"] = record_index
            return out

        def _is_nullish(v) -> bool:
            import math

            if v is None:
                return True
            try:
                if type(v).__name__ == "NAType":
                    return True
            except Exception:
                pass
            try:
                return isinstance(v, float) and math.isnan(v)
            except Exception:
                return False

        # Flatten chunk to row dicts (in-worker only).
        t_flat0 = time.perf_counter()
        rows_main: list[dict] = []
        sub_rows_tot: dict[str, list[dict]] = {}
        excepted_tot: dict[str, list[dict]] = {k: [] for k in except_keys}
        colname_cache: dict = {}

        records_ok = 0
        records_failed = 0
        errors: list[dict] = []

        for i, rec in enumerate(jsons or []):
            idx = index_offset + i
            ctx = None
            if isinstance(record_contexts, (list, tuple)) and i < len(record_contexts):
                maybe = record_contexts[i]
                if isinstance(maybe, dict):
                    ctx = maybe

            try:
                row, sub_rows, excepted = flatten_nested_json_with_list_rows(
                    rec,
                    index_key=index_key,
                    index=idx,
                    except_keys=except_set,
                    sep=sep,
                    colname_cache=colname_cache,
                )
            except Exception as e:
                records_failed += 1
                errors.append(
                    {
                        "index": int(idx),
                        "type": type(e).__name__,
                        "message": str(e),
                        "traceback": traceback.format_exc(),
                    }
                )
                continue

            records_ok += 1
            rows_main.append(row)

            for k, rows in (sub_rows or {}).items():
                if rows:
                    sub_rows_tot.setdefault(str(k), []).extend(rows)

            for k in except_keys:
                try:
                    if k in excepted:
                        excepted_tot.setdefault(k, []).append(_build_excepted_row(str(k), excepted[k], row, ctx))
                except Exception as e:
                    errors.append(
                        {
                            "index": int(idx),
                            "type": type(e).__name__,
                            "message": f"excepted_collect_failed: {e}",
                        }
                    )

        flatten_ms = int(round((time.perf_counter() - t_flat0) * 1000.0))

        # Freeze/hybrid(frozen): filter unknown columns and pack them into extra JSON string per row.
        #
        # - allowed_map is keyed by the *final* table name (base + sub path) as used by the parent pipeline.
        # - If a table is missing from allowed_map, we treat it as "new table": keep all columns (no packing).
        if extra_canon:
            def _pack_rows_for_table(table_name: str, rows: list[dict]) -> None:
                if not rows:
                    return
                keep = allowed_map.get(str(table_name)) if isinstance(allowed_map, dict) else None
                if keep is None:
                    # Ensure column exists in the TSV schema for consistency, even if unused.
                    for r in rows:
                        if isinstance(r, dict) and extra_canon not in r:
                            r[extra_canon] = None
                    return

                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    extras: dict[str, object] | None = None
                    for k in list(r.keys()):
                        ks = k if type(k) is str else str(k)
                        if ks == index_key or ks == extra_canon:
                            continue
                        if ks in keep:
                            continue
                        v = r.get(k)
                        if not _is_nullish(v):
                            if extras is None:
                                extras = {}
                            extras[ks] = v
                        r.pop(k, None)
                    r[extra_canon] = _json_dumps_best_effort(extras) if extras else None

            # main table
            if rows_main:
                _pack_rows_for_table(base_table, rows_main)

            # sub tables
            if sub_rows_tot:
                for sub_key, rows in sub_rows_tot.items():
                    if not rows:
                        continue
                    sub_key_norm = str(sub_key).replace(".", sep)
                    tname = f"{base_table}{sep}{sub_key_norm}" if base_table else sub_key_norm
                    _pack_rows_for_table(tname, rows)

            # excepted tables
            if excepted_tot:
                for ex_key, rows in excepted_tot.items():
                    if not rows:
                        continue
                    ex_key_norm = str(ex_key).replace(".", sep)
                    tname = f"{base_table}{sep}excepted{sep}{ex_key_norm}" if base_table else ex_key_norm
                    _pack_rows_for_table(tname, rows)

        # Write TSV files per table.
        t_tsv0 = time.perf_counter()
        from . import manage as _manage

        escape = _manage._mysql_escape_load_data_value

        def _columns_from_rows(rows: list[dict]) -> list[str]:
            cols_non_null: set[str] = set()
            for r in rows:
                if not isinstance(r, dict):
                    continue
                for k, v in r.items():
                    ks = k if type(k) is str else str(k)
                    if ks == index_key or not _is_nullish(v):
                        cols_non_null.add(ks)
            cols_non_null.add(index_key)
            # Keep existing semantics: id first, then stable sort.
            out = [index_key] + sorted([c for c in cols_non_null if c != index_key])
            if extra_canon and extra_canon not in set(out):
                out.append(extra_canon)
            return out

        run_tag = uuid.uuid4().hex[:12]
        workdir = tempfile.mkdtemp(prefix=f"kisti_flatten_{run_tag}_", dir=tmp_dir)

        def _write_tsv(rows: list[dict], cols: list[str], stem: str) -> dict:
            safe_stem = "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in str(stem))[:80]
            path = os.path.join(workdir, f"{safe_stem}_{uuid.uuid4().hex[:10]}.tsv")
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                write = f.write
                cols_local = list(cols)
                for r in rows:
                    rg = r.get
                    write("\t".join(escape(rg(col)) for col in cols_local) + "\n")
            return {"path": path, "columns": list(cols), "rows": int(len(rows))}

        main_info = None
        if rows_main:
            cols = _columns_from_rows(rows_main)
            main_info = _write_tsv(rows_main, cols, "main")

        subs: dict[str, dict] = {}
        for sub_key, rows in (sub_rows_tot or {}).items():
            if not rows:
                continue
            cols = _columns_from_rows(rows)
            subs[str(sub_key)] = _write_tsv(rows, cols, f"sub_{sub_key}")

        excepted: dict[str, dict] = {}
        for ex_key, rows in (excepted_tot or {}).items():
            if not rows:
                continue
            cols = _columns_from_rows(rows)
            excepted[str(ex_key)] = _write_tsv(rows, cols, f"except_{ex_key}")

        tsv_write_ms = int(round((time.perf_counter() - t_tsv0) * 1000.0))

        return {
            "ok": True,
            "index_offset": int(index_offset),
            "records_ok": int(records_ok),
            "records_failed": int(records_failed),
            "errors": list(errors),
            "timings_ms": {
                "flatten_ms": int(flatten_ms),
                "tsv_write_ms": int(tsv_write_ms),
            },
            "workdir": str(workdir),
            "main": main_info,
            "subs": subs,
            "excepted": excepted,
        }
    except Exception as e:
        return {
            "ok": False,
            "error": {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            },
        }

def separate_excepted(jsons, except_keys, sep='__'):
    _jsons = jsons.copy()
    for key in except_keys:
        __keys = key.split(sep)
        for i, _json in enumerate(_jsons):
            _temp = _jsons[i]
            for _key in __keys[:-1]:
                if not isinstance(_temp, dict) or _key not in _temp:
                    _temp = None
                    break
                _temp = _temp[_key]
            if _temp is None:
                continue
            _temp.pop(__keys[-1], None)
    return _jsons


def read_dict_from_zip(zip_path, json_file_name, report=None):
    """
    Reads a dictionary from a JSON file stored within a ZIP archive.

    Parameters:
    zip_path (str): The path to the ZIP file.
    json_file_name (str): The name of the JSON file within the ZIP archive.

    Returns:
    dict: The dictionary read from the JSON file.
    """
    import zipfile
    import orjson
    # from io import BytesIO
    
    # Initialize an empty dictionary
    extracted_dict = {}

    # Open the ZIP file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Check if the JSON file exists in the ZIP
        if json_file_name in zip_ref.namelist():
            # Read the JSON file from the ZIP archive
            with zip_ref.open(json_file_name) as json_file:
                # Load the file content as a dictionary
                extracted_dict = orjson.loads(json_file.read())
        else:
            if report:
                report.warn(
                    stage="read_dict_from_zip",
                    message="JSON file not found in ZIP archive",
                    zip_path=zip_path,
                    json_file_name=json_file_name,
                )
            else:
                print(f"The file {json_file_name} does not exist in the ZIP archive.")

    return extracted_dict


def extract_rows_from_jsons(
    jsons,
    *,
    index_key: str = "id",
    except_keys=None,
    sep: str = "__",
    report=None,
    quarantine=None,
    index_offset: int = 0,
    record_contexts=None,
    parallel_workers: int | None = None,
):
    """
    Extract row dicts from JSON records (main + sub tables) without building DataFrames.

    This is used for:
    - parallel flattening (ProcessPool)
    - streaming TSV generation (no pandas) + LOAD DATA LOCAL INFILE
    """
    if except_keys is None:
        except_keys = []
    # Normalize once (string keys, trimmed) so flatten/collection keys are consistent.
    except_keys = [str(k).strip() for k in except_keys if str(k).strip()]
    except_set = set(except_keys)

    try:
        index_offset = int(index_offset or 0)
    except Exception:
        index_offset = 0

    try:
        parallel_workers = int(parallel_workers) if parallel_workers is not None else 0
    except Exception:
        parallel_workers = 0

    rows_main: list[dict] = []
    sub_rows_tot: dict[str, list[dict]] = {}
    excepted_tot = {key: [] for key in except_keys}
    # Cache column name prefixing across records (major win for homogeneous JSON arrays).
    colname_cache: dict = {}

    def _json_dumps_best_effort(value):
        try:
            import orjson

            return orjson.dumps(value).decode("utf-8")
        except Exception:
            import json

            try:
                return json.dumps(value, ensure_ascii=False)
            except Exception:
                return json.dumps(str(value), ensure_ascii=False)

    def _build_excepted_row(path: str, value, row: dict, context) -> dict:
        out: dict = {}
        if isinstance(value, dict):
            out.update(value)
        else:
            out["value"] = value

        out[index_key] = row.get(index_key)
        out["__except_path__"] = str(path)
        out["__except_raw_type__"] = type(value).__name__
        out["__except_raw_json__"] = _json_dumps_best_effort(value)

        if isinstance(context, dict):
            source_path = context.get("source_path")
            if source_path is not None:
                out["__source_path__"] = source_path
            source_member = context.get("source_member")
            if source_member is not None:
                out["__source_member__"] = source_member
            line_no = context.get("line_no")
            if line_no is not None:
                out["__line_no__"] = line_no
            record_index = context.get("record_index")
            if record_index is not None:
                out["__record_index__"] = record_index
        return out

    def _handle_record_ok(row, sub_rows, excepted, *, record_context=None):
        if report:
            report.bump("records_ok", 1)
        rows_main.append(row)

        for key, rows in (sub_rows or {}).items():
            if not rows:
                continue
            sub_rows_tot.setdefault(key, []).extend(rows)

        for key in except_keys:
            try:
                if key in excepted:
                    excepted_tot.setdefault(key, []).append(
                        _build_excepted_row(str(key), excepted[key], row, record_context)
                    )
            except Exception as e:
                if report:
                    report.exception(
                        stage="extract_rows_from_jsons",
                        message="Failed to collect excepted branch",
                        exc=e,
                        key=key,
                    )
                if quarantine:
                    try:
                        quarantine.write(
                            stage="extract_rows_from_jsons.excepted",
                            record={},
                            exc=e,
                            key=key,
                        )
                    except Exception:
                        pass

    def _handle_record_fail(i: int, record, err):
        if report:
            report.bump("records_failed", 1)
            try:
                report.warn(
                    stage="extract_rows_from_jsons",
                    message="Failed to flatten JSON record",
                    index=int(i),
                    error=err,
                )
            except Exception:
                pass
        if quarantine:
            try:
                quarantine.write(stage="extract_rows_from_jsons.flatten", record=record, index=i, exc=RuntimeError(str(err)))
            except Exception:
                pass

    use_parallel = parallel_workers is not None and int(parallel_workers or 0) >= 2 and len(jsons) >= 2
    if use_parallel:
        processed = 0
        try:
            from concurrent.futures import ProcessPoolExecutor

            except_keys_tuple = tuple(except_keys or [])
            chunksize = max(1, min(256, len(jsons) // (int(parallel_workers) * 4) or 1))
            args_iter = ((index_offset + i, _json) for i, _json in enumerate(jsons))
            with ProcessPoolExecutor(
                max_workers=int(parallel_workers),
                initializer=_init_flatten_nested_json_with_list_rows_worker,
                initargs=(index_key, except_keys_tuple, sep),
            ) as ex:
                for out in ex.map(_safe_flatten_nested_json_with_list_rows_worker_v2, args_iter, chunksize=chunksize):
                    processed += 1
                    idx, ok, row, sub_rows, excepted, err = out
                    local_i = processed - 1
                    context = None
                    if isinstance(record_contexts, (list, tuple)) and local_i < len(record_contexts):
                        maybe_ctx = record_contexts[local_i]
                        if isinstance(maybe_ctx, dict):
                            context = maybe_ctx
                    if ok:
                        _handle_record_ok(row, sub_rows, excepted, record_context=context)
                    else:
                        try:
                            rec = jsons[local_i]
                        except Exception:
                            rec = {}
                        _handle_record_fail(index_offset + local_i, rec, err)
        except Exception as e:
            if report:
                try:
                    msg = "Parallel flatten failed; falling back to sequential for remaining records"
                    if isinstance(e, PermissionError):
                        msg = (
                            "Parallel flatten unavailable (PermissionError starting worker processes); "
                            "falling back to sequential for remaining records"
                        )
                    report.warn(
                        stage="extract_rows_from_jsons.parallel",
                        message=msg,
                        error={"type": type(e).__name__, "message": str(e)},
                        processed=int(processed),
                        total=int(len(jsons)),
                    )
                except Exception:
                    pass
            # Best-effort: process remaining sequentially to avoid losing the batch.
            for i in range(processed, len(jsons)):
                _json = jsons[i]
                try:
                    row, sub_rows, excepted = flatten_nested_json_with_list_rows(
                        _json,
                        index=index_offset + i,
                        index_key=index_key,
                        except_keys=except_set,
                        sep=sep,
                        colname_cache=colname_cache,
                    )
                except Exception as e2:
                    _handle_record_fail(index_offset + i, _json, {"type": type(e2).__name__, "message": str(e2)})
                    continue
                context = None
                if isinstance(record_contexts, (list, tuple)) and i < len(record_contexts):
                    maybe_ctx = record_contexts[i]
                    if isinstance(maybe_ctx, dict):
                        context = maybe_ctx
                _handle_record_ok(row, sub_rows, excepted, record_context=context)
        return rows_main, sub_rows_tot, excepted_tot

    # Sequential path
    for i, _json in enumerate(jsons):
        try:
            row, sub_rows, excepted = flatten_nested_json_with_list_rows(
                _json,
                index=index_offset + i,
                index_key=index_key,
                except_keys=except_set,
                sep=sep,
                colname_cache=colname_cache,
            )
        except Exception as e:
            _handle_record_fail(index_offset + i, _json, {"type": type(e).__name__, "message": str(e)})
            continue
        context = None
        if isinstance(record_contexts, (list, tuple)) and i < len(record_contexts):
            maybe_ctx = record_contexts[i]
            if isinstance(maybe_ctx, dict):
                context = maybe_ctx
        _handle_record_ok(row, sub_rows, excepted, record_context=context)

    return rows_main, sub_rows_tot, excepted_tot


def extract_data_from_jsons(
    jsons,
    index_key="id",
    except_keys=None,
    sep="__",
    report=None,
    quarantine=None,
    *,
    index_offset: int = 0,
    record_contexts=None,
    parallel_workers: int | None = None,
):
    """
    Aggregates data from a list of JSON objects into a primary DataFrame and a set of subsidiary DataFrames.
    Each JSON object is flattened, with its nested structures and lists transformed into DataFrames.
    The primary DataFrame accumulates data not contained within lists, while subsidiary DataFrames
    accumulate data from parts of the JSON that contained lists.

    Parameters:
    jsons (Series of dict): A Series of JSON objects (dictionaries) to process. Each JSON object can contain
                          nested structures and lists.

    Returns:
    tuple: A tuple containing two elements:
           - df (pd.DataFrame): The aggregated primary DataFrame containing data extracted from all the
                                JSON objects, excluding data from nested lists.
           - df_subs (dict): A dictionary of DataFrames, where each key corresponds to a part of the JSON
                             that contained a list, and each value is a DataFrame aggregating the data
                             from that list across all JSON objects.

    Note:
    - The function assumes that `flatten_nested_json_with_list` returns a DataFrame for non-list data
      and a dictionary of DataFrames for list-contained data from a single JSON object.
    - DataFrames are concatenated vertically, so all JSON objects and their list-contained data are
      aggregated into the respective DataFrames.
    - An index is added to each DataFrame derived from a JSON object to maintain order and reference
      back to the original JSON object in the list.
    """
    if except_keys is None:
        except_keys = []

    if report:
        report.bump("records_total", len(jsons) if hasattr(jsons, "__len__") else 0)

    def cleanse_dummies(df_subs, sep):
        """
        Best-effort cleanup for dummy list marker columns.

        Older logic used value_counts() across all columns which is expensive and can fail
        on unhashable values. Restrict to columns that look like list markers.
        """
        marker = f"{sep}List"
        for sub in list(df_subs.keys()):
            df_sub = df_subs[sub]
            drop_cols = []
            for col in list(df_sub.columns):
                if marker not in str(col):
                    continue
                series = df_sub[col]
                # Drop only when all non-null values are empty lists.
                try:
                    all_empty = True
                    for v in series:
                        if v is None or (isinstance(v, float) and np.isnan(v)):
                            continue
                        if isinstance(v, list) and len(v) == 0:
                            continue
                        all_empty = False
                        break
                    if all_empty:
                        drop_cols.append(col)
                except Exception:
                    continue
            if drop_cols:
                df_subs[sub] = df_sub.drop(columns=drop_cols)
        return df_subs

    rows_main, sub_rows_tot, excepted_tot = extract_rows_from_jsons(
        jsons,
        index_key=index_key,
        except_keys=except_keys,
        sep=sep,
        report=report,
        quarantine=quarantine,
        index_offset=index_offset,
        record_contexts=record_contexts,
        parallel_workers=parallel_workers,
    )
    
    if len(rows_main) == 0:
        if report:
            report.warn(stage="extract_data_from_jsons", message="No records succeeded; returning empty results")
        return pd.DataFrame([]), {}, excepted_tot

    df_tot = pd.DataFrame.from_records(rows_main).dropna(axis=1, how="all")
    if index_key in df_tot.columns:
        df_tot = df_tot[[index_key] + [c for c in df_tot.columns if c != index_key]]

    df_subs_tot: dict[str, pd.DataFrame] = {}
    for key, rows in sub_rows_tot.items():
        if not rows:
            continue
        df_sub = pd.DataFrame.from_records(rows).dropna(axis=1, how="all").reset_index(drop=True)
        if index_key in df_sub.columns:
            df_sub = df_sub[[index_key] + [c for c in df_sub.columns if c != index_key]]
        df_subs_tot[key] = df_sub

    df_subs_tot = cleanse_dummies(df_subs_tot, sep)

    return df_tot, df_subs_tot, excepted_tot


def read_dict_from_gz(gz_path):
    """
    Reads a dictionary from a JSON file compressed with gzip.

    Parameters:
    gz_path (str): The path to the gzip-compressed JSON file.

    Returns:
    Series: The Series of json read from the compressed JSON file.
    """
    import gzip
    import orjson
    
    # Initialize an empty dictionary
    extracted_dict = {}
    # Open the gzip-compressed file
    with gzip.open(gz_path, 'rt', encoding='utf-8') as gz_file:
        # Load the JSON content into a dictionary
        json_strs = gz_file.read().split('\n')
        df_data = pd.DataFrame(json_strs, columns=['raw_str'])
        msk = df_data != ''
        df_data = df_data[msk].dropna()
        df_data['json'] =  df_data['raw_str'].apply(orjson.loads)

    return df_data['json']


import pandas as pd
import numpy as np

def json_to_key_pairs(json_data, parent=None, parent_type=None, result=None, sep='__'):
    _Types = {
        'l': 'List',
        'lv': 'List of Value',
        'ld': 'List of Dict',
        'vld': 'Value in List of Dict',
        'd': 'Dict',
        'v': 'Value',
    }

    def get_res(__json_data, parent=None, parent_type=None, result=None, sep='__'):
        if result is None:
            result = [(_Types['d'], type(__json_data).__name__, np.nan, parent)]
        
        if isinstance(__json_data, dict):
            for key, value in __json_data.items():
                child_type = _Types['d'] if isinstance(value, dict) else _Types['lv'] if isinstance(value, list) else _Types['v']
                if isinstance(value, list) and any(isinstance(i, dict) for i in value):
                    child_type = _Types['ld']
                
                new_parent = key if parent is None else parent + sep + key
                result.append((child_type, type(__json_data).__name__, parent, new_parent))
                get_res(value, new_parent, child_type, result)
        
        elif isinstance(__json_data, list):
            item_type = _Types['ld'] if any(isinstance(i, dict) for i in __json_data) else _Types['lv']
            for item in __json_data:
                if isinstance(item, dict):
                    for key in item:
                        new_parent = key if parent is None else parent + sep + key
                        result.append((_Types['vld'], type(__json_data).__name__, parent, new_parent))
                        get_res(item[key], new_parent, _Types['vld'], result)
            if parent_type != _Types['lv'] and parent is not None:
                result.append((_Types['l'], type(__json_data).__name__, parent, parent + sep + _Types['l']))
                
        return result

    res = get_res(json_data)
    res_df = pd.DataFrame(res).fillna(parent).dropna()
    return [tuple(l) for l in res_df.values.tolist()]


def key_pair_to_df(key_pairs, sep='__'):
    """
    Optimized version of key_pair_to_df.
    """
    cols = ['type', 'dtype', 'parent', 'branch']
    temp = pd.DataFrame(list(set(key_pairs)), columns=cols)
    temp['count'] = 1
    
    # Grouping by parent and branch, and counting occurrences
    temp_grouped = temp.groupby(['parent', 'branch', 'type']).sum()['count'].unstack(fill_value=0)
    
    # Re-organize the result into a MultiIndex DataFrame
    res = pd.DataFrame(index=temp_grouped.index)
    res['type'] = temp_grouped.idxmax(axis=1)
    
    return res.reset_index().drop_duplicates().reset_index(drop=True)


def excepted_regularization(_jsons, types, base_key='', sep='__'):
    """
    A
    """
    def __init():
        _res = {}
        for branch in types.index:
            keys = branch.split(sep)
            __type = types[branch]
            __res = _res
            for key in keys[:-1]:
                if isinstance(__res, list):
                    __res = __res[0]
                __res = __res.setdefault(key, {})

            fkey = keys[-1]
            if isinstance(__res, list):
                __res = __res[0]

            if __type == 'Value':
                __res[fkey] = ''
            elif __type == 'List':
                __res[fkey] = []
            elif __type == 'List of Dict':
                __res[fkey] = [{}]
            elif __type == 'Dict':
                __res[fkey] = {}
        return _res


    def remove_none(obj):
        if isinstance(obj, dict):
            return {k: remove_none(v) for k, v in obj.items() if v is not None}
        elif isinstance(obj, list):
            return [remove_none(item) for item in obj if item is not None]
        else:
            return obj
    

    def insert_value(data, __res, full_key=''):
        if isinstance(data, dict):
            for k, v in data.items():
                _full_key = full_key + sep + k if full_key else k
                if types[_full_key] == 'Dict':
                    insert_value(v, __res[k], _full_key)
                else:
                    __res[k] = v

        elif isinstance(data, list):
            for item in data:
                insert_value(item, __res, full_key)

    _jsons = [remove_none(x) for x in _jsons]

    result = []
    types = types.iloc[1:]
    for _json in tqdm(_jsons, desc='\t', mininterval=1):
        _res = __init().copy()
        insert_value(_json, _res, base_key)
        result.append(_res)

    return result


def json_parsing(
    jsons_data,
    origin="",
    sep="__",
    forced=None,
    index_key="id",
    except_keys=None,
    report=None,
    quarantine=None,
):
    '''
    json_parsing 함수는 JSON 데이터를 분석하고 정규화하여 여러 데이터프레임을 반환합니다.
    
    매개변수:
    - jsons_data: JSON 데이터 리스트 (list of dicts).
    - origin: 선택적 매개변수로, JSON 정규화 중 출처를 명시하기 위한 문자열.
    - forced: 특정 키에 대해 강제 적용할 값이 있는 경우 사용하는 딕셔너리.
    - index_key: 각 JSON 객체의 고유 식별자 역할을 하는 키. 기본값은 'id'.
    
    반환값:
    - df: 정규화된 JSON 데이터로부터 추출된 메인 데이터프레임.
    - df_subs: JSON에서 추출된 서브 데이터프레임 (nested 구조의 데이터).
    - excepted: 추출에서 제외된 JSON 일부 (구조가 복잡한 부분 등).
    - sample: 정규화된 JSON 데이터의 첫 번째 샘플.

    Usage:
    df, df_subs, excepted, sample = processing.json_parsing(json_data, origin=origin, forced={}, index_key='id')
    '''
    if forced is None:
        forced = {}
    if except_keys is None:
        except_keys = []

    # STEP 1. Analyze JSON Structure
    print('STEP 1. Analyze JSON Structure:')
    key_pairs = json_to_key_pairs(jsons_data, '', sep=sep)
    key_pairs_df = key_pair_to_df(key_pairs, sep=sep)
    types = key_pairs_df.set_index('branch')['type']
    
    # STEP 2. Regularize JSON
    print('STEP 2. Regularize JSON:')
    excepted_reg = excepted_regularization(jsons_data, types, origin, sep=sep)
    
    # STEP 3. Extract Data from Regularized JSON
    print('STEP 3. Extract Data from Regularized JSON:')
    df, df_subs, excepted = extract_data_from_jsons(
        excepted_reg,
        index_key,
        except_keys,
        sep=sep,
        report=report,
        quarantine=quarantine,
    )
    
    return df, df_subs, excepted, excepted_reg[0]


def save_data(dfs, data_config):

    df, df_subs = dfs
    from .config import coerce_data_config, join_path

    data_config = coerce_data_config(data_config)
    PATH = data_config["PATH"]
    key_sep = data_config.get("KEY_SEP", data_config.get("SEP", "__"))
    table_name = data_config["table_name"]

    idx = 1
    suffix = 'MAIN'
    fname = join_path(PATH, f"{table_name}{key_sep}{suffix}.parquet")
    if data_config.get("fname_index", False):
        fname = join_path(PATH, f"{idx:02d}{key_sep}{table_name}{key_sep}{suffix}.parquet")
    try:
        df.to_parquet(fname)
        print(f"'{fname}' is successfully saved.")
        idx += 1
        
        for key in df_subs.keys():
            suffix = f"SUB{key_sep}{key}"
            fname = join_path(PATH, f"{table_name}{key_sep}{suffix}.parquet")
            if data_config.get("fname_index", False):
                fname = join_path(PATH, f"{idx:02d}{key_sep}{table_name}{key_sep}{suffix}.parquet")
            try:
                try:
                    df_subs[key].reset_index(drop=True).to_parquet(fname)
                except:
                    def fix_type(val):
                        '''
                        주어진 값을 문자열로 변환.
                        - bytes 타입이면 UTF-8로 디코딩 (문제가 있으면 errors='replace' 적용)
                        - 그 외에는 str()을 통해 문자열로 변환.
                        '''
                        if isinstance(val, bytes):
                            try:
                                return val.decode("utf-8")
                            except UnicodeDecodeError:
                                return val.decode("utf-8", errors='replace')
                        else:
                            return str(val)
                    df_subs[key] = df_subs[key].map(fix_type)
                    df_subs[key].reset_index(drop=True).to_parquet(fname)
                    
                idx += 1
                print(f"'{fname}' is successfully saved.")
            except:
                print(f'Fail to save the {fname}')
    except:
        print(f"Fail to save the {fname}")


def rename_columns(df, df_subs, options):
    # 옵션에서 구분자를 '_'로 대체
    if options.get('replace_delimiters', True):
        df.columns = [col.replace('__', '_').replace('.', '_') for col in df.columns]
        for field in df_subs:
            df_subs[field].columns = [col.replace('__', '_').replace('.', '_') for col in df_subs[field].columns]

    # df_subs의 열 이름을 처리하는 함수 정의
    def process_col_name(col_name, sub_df_name):
        # df_subs의 col_name이 테이블 구조를 따른다면 하위 필드만 사용
        if options.get('sub_df_simplify', True):
            if col_name.startswith(sub_df_name + '_'):
                return col_name[len(sub_df_name) + 1:]  # 필드 이름만 남김
        return col_name

    # df_subs에 대해 열 이름 변경 처리
    for sub_df_name, sub_df in df_subs.items():
        sub_df.columns = [process_col_name(col, sub_df_name) for col in sub_df.columns]
        df_subs[sub_df_name] = sub_df  # 수정된 sub_df 저장


def one_hot_encoding(df, table_name="MAIN", exceptions=None, max_unique_num=5):
    if exceptions is None:
        exceptions = []
    def one_hot(df, col, exceptions):
        # Get the unique values in the column
        unique_vals = sorted(df[col].explode().unique())  # 고유 값을 정렬
        
        # Limit to the values not in exceptions
        unique_vals = [val for val in unique_vals if val not in exceptions]
        
        # Create one-hot encoded columns for each unique value
        for val in unique_vals:
            df[f'{col}_{val}'] = df[col].apply(lambda x: 1 if isinstance(x, (list, np.ndarray, set)) and val in x else 0)
        del df[col]  # 원본 열 삭제

    # Iterate through columns
    i = 0
    for col in df.columns:
        # NaN 처리: 리스트나 배열이 아닌 경우 빈 리스트로 변환
        df[col] = df[col].apply(lambda x: x if isinstance(x, (list, np.ndarray, set)) else x if pd.isnull(x) else x)

        # Check if the column contains lists and unique values are less than the threshold
        if df[col].apply(lambda x: isinstance(x, (list, np.ndarray, set))).any() and len(df[col].explode().unique()) <= max_unique_num:
            if i == 0:
                print(f"In '{table_name}' table,")
                i += 1
            print(f"\tConvert '{col}' to one-hot")
            one_hot(df, col, exceptions)
