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
           "read_dict_from_zip", "extract_data_from_jsons", "read_dict_from_gz", "json_to_key_pairs",
          "key_pair_to_df", "excepted_regularization", "separate_excepted", "json_parsing"]

def conv_HTML_entities(content, replace_list=['p', 'sub', 'sup', 'i', 'b', ], 
                       rounds=[('<', '>'), ('</', '>')], 
                       trans=[('%_lt_;', '%_gt_;'), ('%_lt_;/', '%_gt_;')], verbose=False):
    import re
    content_conv = content[:]
    for r in tqdm(replace_list, desc='Convert HTML Entities: '):
        for i, ro in enumerate(rounds):
            _tag = f"{ro[0]}{r}{ro[1]}"
            _tag_to = f"{trans[i][0]}{r}{trans[i][0]}"
            content_conv = re.sub(_tag, _tag_to, content_conv)
            if verbose:
                res = re.findall(_tag, content)
                print(len(res), _tag, 'is converted from', gz_file_name, 'in', f'{f}.')
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

def read_NSF_from_zip(zipf, path, key_on, ext_keys=[]):
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
    
    data = []
    sub_datas = {}
    logs = []
    for ek in ext_keys:
        sub_datas[ek] = []
    
    with ZipFile(path+zf, 'r') as zipObj:
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
                except: # Error Log
                    logs.append([zf, fileName])
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


def flatten_json_separate_lists(nested_json, except_keys=[], sep='__', parent=''):
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
    single_values = {}
    multiple_values = {}
    excepted_values = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                if a in except_keys:
                    excepted_values[name + a] = x[a]
                else:
                    flatten(x[a], name + a + sep)
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


def flatten_nested_json_with_list(_json, index_key='id', index=0, except_keys=[], sep='__'):

    single, multiples, excepted = flatten_json_separate_lists(_json, except_keys=except_keys, sep=sep)
    _df = pd.DataFrame(single, index=[index])
    _id = _df.loc[index, index_key]
    df_subs = multiples_to_dataframes(multiples)
    df_subs_add_idx = add_index_column_to_dfs(df_subs, index_key, _id)

    for idx, key in enumerate(except_keys):
        try:
            excepted[key][index_key] = _id
        except:
            excepted = {key:{index_key:_id}}
    return _df, df_subs_add_idx, excepted

def separate_excepted(jsons, except_keys, sep='__'):
    _jsons = jsons.copy()
    for key in except_keys:
        __keys = key.split(sep)
        for i, _json in enumerate(_jsons):
            _temp = _jsons[i]
            for _key in __keys[:-1]:
                _temp = _temp[_key]
            _temp.pop(__keys[-1], None)
    return _jsons


def read_dict_from_zip(zip_path, json_file_name):
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
                extracted_dict = orjson.loads(json_file)
        else:
            print(f"The file {json_file_name} does not exist in the ZIP archive.")

    return extracted_dict


def extract_data_from_jsons(jsons, index_key='id', except_keys=[], sep='__'):
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
    from tqdm import tqdm

    for i, _json in enumerate(tqdm(jsons, desc='\t', #ncols=90, 
                      mininterval=1)):
        # Process each JSON object to flatten it and extract data into DataFrames
        _df, _df_subs, excepted = flatten_nested_json_with_list(_json, index=i, index_key=index_key, except_keys=except_keys, sep=sep)
        if i == 0:
            df_tot = []
            df_subs_tot = {}
            excepted_tot = {}
            for key in _df_subs.keys():
                df_subs_tot[key] = []
            for key in except_keys:
                excepted_tot[key] = []
        # Attempt to concatenate the new DataFrames with the aggregated ones
        df_tot.append(_df)
        for key in df_subs_tot.keys():
            if len(_df_subs[key]) > 0:
                df_subs_tot[key].append(_df_subs[key])
        for key in except_keys:
            try:
                if len(excepted[key]) > 0:
                    excepted_tot[key].append(excepted[key])
            except:
                print('err log')
                pass
    
    df_tot = pd.concat(df_tot).dropna(axis=1, how='all')
    del_keys = []
    for key in df_subs_tot.keys():
        if len(df_subs_tot[key]) > 0:
            __res = pd.concat(df_subs_tot[key]).dropna(axis=1, how='all').reset_index(drop=True)
            df_subs_tot[key] = __res
        else:
            del_keys.append(key)
    for key in del_keys:
        df_subs_tot.pop(key)
    for key in except_keys:
        try:
            excepted_tot[key].append(excepted[key])
        except:
            print('err log')
            pass

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

    result = []
    types = types.iloc[1:]
    for _json in tqdm(_jsons, desc='\t', mininterval=1):
        _res = __init().copy()
        insert_value(_json, _res, base_key)
        result.append(_res)

    return result


def json_parsing(jsons_data, origin='', sep='__', forced={}, index_key='id', except_keys=[]):
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
    df, df_subs, excepted = extract_data_from_jsons(excepted_reg, index_key, except_keys, sep=sep)
    
    return df, df_subs, excepted, excepted_reg[0]


def save_data(dfs, data_config):

    df, df_subs = dfs
    PATH, KEYS, SEP, table_name = data_config['PATH'], data_config['KEYS'], data_config['SEP'], data_config['table_name']

    idx = 1
    suffix = 'MAIN'
    fname = f'{PATH}{table_name}{SEP}{suffix}.ftr'
    if data_config['fname_index']:
        fname = f'{PATH}{idx:02d}{SEP}{table_name}{SEP}{suffix}.ftr'
    try:
        df.to_feather(fname)
        print(f"'{fname}' is successfully saved.")
        idx += 1
        
        for key in df_subs.keys():
            suffix = f"SUB{SEP}{key}"
            fname = f'{PATH}{table_name}{SEP}{suffix}.ftr'
            if data_config['fname_index']:
                fname = f'{PATH}{idx:02d}{SEP}{table_name}{SEP}{suffix}.ftr'
            try:
                df_subs[key].reset_index(drop=True).to_feather(fname)
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


def one_hot_encoding(df, table_name='MAIN', exceptions=[], max_unique_num=5):
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