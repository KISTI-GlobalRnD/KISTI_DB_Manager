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
          "key_pair_to_df", "excepted_regularization", "separate_excepted"]

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

def flatten_json_separate_lists(nested_json, except_keys=[], separator='__', parent=''):
    """
    Flattens a nested JSON object (Python dictionary) and separates the output into
    two dictionaries: one for keys with single (non-list) values, and another for keys
    with multiple values (lists), with keys indicating the path through the original
    nested structure.

    Parameters:
    nested_json (dict): The JSON object (dictionary) to flatten.
    separator (str): The separator to use for concatenating nested keys. Default is '.'.

    Returns:
    tuple: A tuple containing two dictionaries. The first dictionary contains flattened
           keys with single (non-list) values. The second dictionary contains flattened
           keys with list values.
    """
    single_values = {}
    multiple_values = {}
    excepted_values = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                if a in except_keys:
                    excepted_values[a] = x[a]
                else:
                    flatten(x[a], name + a + separator)
        elif isinstance(x, list):
            multiple_values[name[:-1*len(separator)]] = x
            # print(name)
        else:
            single_values[name[:-1*len(separator)]] = x

    flatten(nested_json)
    return single_values, multiple_values, excepted_values


def multiples_to_dataframes(multiples, separation='__'):
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
            df.columns = [f'{key}{separation}{col}' if col != key else col for col in df.columns]
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


def flatten_nested_json_with_list(_json, index_key='id', index=0, except_keys=[]):

    single, multiples, excepted = flatten_json_separate_lists(_json, except_keys=except_keys)
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


def extract_data_from_jsons(jsons, index_key='id', except_keys=[]):
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
        _df, _df_subs, excepted = flatten_nested_json_with_list(_json, index=i, index_key=index_key, except_keys=except_keys)
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
            result = [(_Types['d'], np.nan, parent)]
        
        if isinstance(__json_data, dict):
            for key, value in __json_data.items():
                child_type = _Types['d'] if isinstance(value, dict) else _Types['lv'] if isinstance(value, list) else _Types['v']
                try:
                    if any(isinstance(i, dict) for i in value):
                        child_type = _Types['ld']
                except:
                    pass
                if parent == None:
                    new_parent = key
                else:
                    new_parent = parent+sep+key
                result.append((child_type, parent, new_parent))
                get_res(value, new_parent, child_type, result)
        elif isinstance(__json_data, list):
            item_type = _Types['ld'] if any(isinstance(i, dict) for i in __json_data) else _Types['lv']
            # If the list contains dictionaries, treat each key in those dictionaries
            if item_type == _Types['ld']:
                for item in __json_data:
                    if isinstance(item, dict):
                        for key in item:
                            if parent_type == _Types['ld']:
                                item_type = _Types['vld']
                            if parent == None:
                                new_parent = key
                            else:
                                new_parent = parent+sep+key
                            result.append((item_type, parent, new_parent))
                            get_res(item[key], new_parent, item_type, result)
            # For lists of primitives, skip adding them directly but mark the presence of a list
            elif parent_type != _Types['lv']:
                if parent is not None:
                    result.append((_Types['l'], parent, parent+sep+_Types['l']))
        return result
    if isinstance(json_data, list):
        res = []
        for _json_data in tqdm(json_data, desc='\t', #ncols=90, 
                      mininterval=1):
            _res =  get_res(_json_data)
            res += _res
    else:
        res = get_res(json_data)
    res = pd.DataFrame(res).fillna(parent).dropna().values.tolist()
    return [tuple(l) for l in res]


def key_pair_to_df(key_pairs, sep='__'):
    cols = ['type', 'parent', 'branch']
    temp = pd.DataFrame(list(set(key_pairs)), columns=cols).sort_values(cols)
    temp['note'] = ''
    temp['count'] = 1
    
    new_cols = ['parent', 'branch', 'type']
    temp = temp.reset_index().fillna('').groupby(new_cols).sum()['count'].unstack().fillna(0).astype(int)
    temp['cnt'] = temp.sum(axis=1)
    # print(temp)
    res = pd.DataFrame([], index=temp.index)
    res['note'] = ''
    res['type'] = ''
    for idx in temp.index:
        # print(idx)
        _ = temp.loc[idx]
        msk = _ == 1
        if _['cnt'] == 1:
            res.loc[idx, 'type'] = _[msk].index[0]
        else: # First, change all to Final values 
            __note = ";".join(_[msk].index)
            res.loc[idx, 'note'] = __note
            if (__note == 'List of Dict;Value in List of Dict') | (__note == 'Value in List of Dict;List of Dict'):
                # _type = 'Value in List of Dict'
                _type = 'List of Dict'
                # print(idx)
            elif (__note == 'List of Value;List of Dict') | (__note == 'List of Dict;List of Value'):
                _type = 'List of Dict'
            elif (__note == 'Dict;Value in List of Dict') | (__note == 'Value in List of Dict;Dict'):
                _type = 'Dict'
            elif (__note == 'Dict;Value') | (__note == 'Value;Dict'):
                _type = 'Dict'
            else:
                _max_len = max([len(x) for x in _[msk].index])
                _type = [x for x in _[msk].index if len(x) == _max_len][0]
            res.loc[idx, 'type'] = _type

    # Convert VLD to LD when offs are also VLD
    temp = res.reset_index()
    msk0 = temp['type'] == 'Value in List of Dict'
    for i, idx in enumerate(temp[msk0]['branch']):
        msk = (temp['parent'] == idx) #& (temp['type'] == 'Value in List of Dict')
        # Change the parent
        if msk.sum() > 0:
            _idx = temp[msk0].iloc[i][['parent', 'branch']].values
            _idx = tuple(list(_idx))
            res.loc[_idx, 'type'] = 'List of Dict'
    
    temp = res.reset_index()
    msk0 = temp['type'] == 'List of Dict'
    for i, idx in enumerate(temp[msk0]['parent']):
        msk = (temp['branch'] == idx) & (temp['type'] == 'List of Dict')
        if msk.sum() > 0:
            _idx = temp[msk0].iloc[i][['parent', 'branch']].values
            _idx = tuple(list(_idx))
            res.loc[_idx, 'type'] = 'Value in List of Dict'
    # return res.reset_index()

    # Convert VLD to LD when offs are also VLD Again
    temp = res.reset_index()
    msk0 = temp['type'] == 'Value in List of Dict'
    for i, idx in enumerate(temp[msk0]['branch']):
        msk = (temp['parent'] == idx) #& (temp['type'] == 'Value in List of Dict')
        # Change the parent
        if msk.sum() > 0:
            _idx = temp[msk0].iloc[i][['parent', 'branch']].values
            _idx = tuple(list(_idx))
            res.loc[_idx, 'type'] = 'List of Dict'


    # print(res.reset_index().set_index('branch')['type']['static_data__fullrecord_metadata__references__reference__physicalSection'])
    # print(res.reset_index().set_index('branch')['type']['static_data__fullrecord_metadata__references__reference__physicalSection__@physicalLocation'])
    return res.reset_index()#.iloc[1:]


# def key_pair_to_df(key_pairs, unique_set=True, forced={}, sep='__'):
#     from collections import Counter
    
#     cols = ['type', 'parent', 'branch']
#     if unique_set==True:
#         res_df = pd.DataFrame(list(set(key_pairs)), columns=cols).sort_values(cols)
#     else:
#         cols.append('count')
#         key_pairs = [(type, parent, branch, count) for (type, parent, branch), count in Counter(key_pairs).most_common(100000000000)]
#         res_df = pd.DataFrame(key_pairs, columns=cols).sort_values(cols)

#     res_df = res_df.set_index('branch')
#     for idx in res_df.index:
#         if isinstance(res_df.loc[idx, 'type'], pd.Series):
#             msk = res_df.loc[idx, 'type'].str.contains('List')
#             v = res_df.loc[idx, 'type'][msk]
#             res_df.loc[idx, 'type'] = v
    
#     # When excepted, duplicated index exist
#     # So, only revise Multiples delete singles
#     for k, v in forced.items():
#         res_df.loc[k, 'type'] = v
#     res_df = res_df.reset_index().drop_duplicates()[cols]
#     res_df['Rank'] = res_df.branch.apply(lambda x: len(x.split(sep)))
    
#     return res_df.sort_values('

def excepted_regularization(_jsons, types, base_key='', sep='__'):
    def __init():
        _res = {}
        for branch in types.index:
            keys = branch.split(sep)[:]
            __type = types[branch]
            __res = _res
            for key in keys[:-1]:
                if isinstance(__res, list):
                    __res = __res[0]
                try:
                    __res = __res[key]
                except:
                    __res[key] = {}
                    __res = __res[key]
            if len(keys) > 0:
                fkey = keys[-1]
                if isinstance(__res, list):
                    __res = __res[0]
                    
                if __type == 'Value':
                    __res[fkey] = ''
                elif __type == 'List of Value':
                    __res[fkey] = []
                elif __type == 'List of Dict': # Assume I: 'Values in List of Dict' is not empty
                    __res[fkey] = [{}]
                elif __type == 'Value in List of Dict':
                    __res[fkey] = ''
                elif __type == 'Dict':
                    __res[fkey] = {}
                else: # List
                    __res[fkey] = []
                
                if isinstance(__res, list):
                    __res = __res[0]
                
        return _res

    
    def get_value(__data, keys):
        if isinstance(keys, list) and len(keys) > 0:
            if isinstance(__data, dict):
                return get_value(__data[keys[0]], keys[1:])
            elif isinstance(__data, list):
                return get_value(__data[0], keys)
        else:
            # print(__data)  # For debugging; you might want to remove this in the final version
            return __data if not isinstance(__data, dict) else __data.copy()

    
    def insert_value(data, __res, full_key=''):
        if isinstance(data, dict):
            for k, v in data.items():
                if full_key != '':
                    _full_key = full_key+sep+k
                else:
                    _full_key = k

                if types[_full_key] == 'Value':
                    # print('V**', v, _full_key, '\n')
                    __res[k] = v # Init is ''
                elif types[_full_key] == 'Value in List of Dict':
                    # print('VLD**', v, k, data, '\n')
                    __res[k] = v # Init is ''
                elif types[_full_key] == 'Dict':
                    # if _full_key == 'static_data__summary__titles__title':
                        # print('D**', v, _full_key, '\n')
                    insert_value(v, __res[k], _full_key)
                elif types[_full_key] == 'List of Dict':
                    # if _full_key == 'static_data__summary__titles__title':
                    # print('LD*', __res, v, k, data)
                    _keys = _full_key.split(sep)
                    if type(v) != list: # '0'번째에 채워넣기
                        format = get_value(__init(), _keys)[0]
                        for k2, v2 in v.items():
                            format[k2] = v2
                        __res[k][0] = format
                    else: # 덮어서 붙여넣기 (해도 되나?)
                        # print('LD*', __res, v, k)
                        _formats = []
                        for _v in v:
                            format = get_value(__init(), _keys)[0]
                            for k2, v2 in _v.items():
                                format[k2] = v2
                            _formats.append(format)
                        # print('$$$$', _formats)
                        __res[k] = _formats
                            
                            # __res[k].append(v)
                elif types[_full_key] == 'List of Value':
                    if type(v) != list: # '0'번째에 채워넣기
                        v = [v]
                    # for _v in v:
                    __res[k] = v
                        # insert_value(_v, __res[k], _full_key)
                else:
                    print('*****', types[_full_key])

        elif isinstance(data, list):
            for item in data:
                if types[full_key] == 'Value':
                    # print('VoL*', item, data, full_key)
                    __res = item # Init is ''
                elif types[full_key] == 'Value in List of Dict':
                    # print('VLDoL*', item, data, full_key)
                    __res = item # Init is ''
                elif types[full_key] == 'Dict':
                    # print('DoL*', item, data,full_key)
                    for k, v in item.items():
                        if full_key != '':
                            _full_key = full_key+sep+k
                        else:
                            _full_key = k
                        insert_value(item, __res[k], _full_key)
                elif types[full_key] == 'List of Dict':
                    # print('LD*', __res, v, k)
                    for k, v in item.items():
                        if full_key != '':
                            _full_key = full_key+sep+k
                        else:
                            _full_key = k
                        insert_value(_v, __res, _full_key)
                    # for _v in item:
                    #     insert_value(_v, __res, _full_key)
                elif types[_full_key] == 'List of Value':
                    __res.append(item)
                    # print('EoL*', item, data,full_key)
                else:
                    print('*****', types[_full_key])
        else:
            if types[full_key] == 'List of Value':
                print('LV*', data, __res, full_key)
                # insert_value(data, __res, full_key)
                __res = data
            elif data == None:
                # 데이타 없어서 걍 넘어가도 기본형식으로 채워짐
                pass
            else:
                print('E*', types[full_key], data, __res, full_key)

    result = []
    types = types.iloc[1:]
    for _json in tqdm(_jsons, desc='\t', #ncols=90, 
                      mininterval=1):#, bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}", mininterval=1):
        _res = __init().copy()
        insert_value(_json, _res, base_key)
        result.append(_res)
    return result


def json_parsing(jsons_data, origin='', forced={}, index_key='id'):
    '''

    return: df, df_subs, df_exc, sample
    '''
    # STEP 1. Analyze Json Structure
    print('STEP 1. Analyze Json Structure:')
    key_pairs = json_to_key_pairs(jsons_data, '')
    key_pairs_df = key_pair_to_df(key_pairs)
    types = key_pairs_df.reset_index().set_index('branch')['type']
    # When The Structure is stiff. with except_keys
    # except_key = ['dynamic_data']
    # df, df_subs, excepted_part = processing.extract_data_from_jsons(jsons[:], index_key, except_keys)
    # When The Structure is unstable.

    # STEP 2.
    print('STEP 2. Regularize Json:')
    excepted_reg = excepted_regularization(jsons_data, types, origin)
    
    # STEP 3.
    print('STEP 3. Extract Data from Regularized json:')
    df_ex, df_ex_subs, excepted_part = extract_data_from_jsons(excepted_reg, index_key)
    return df_ex, df_ex_subs, excepted_part, excepted_reg[0]


def save_data(dfs, data_config):

    df, df_subs = dfs
    PATH, KEY, SEP, table_name = data_config['PATH'], data_config['KEY'], data_config['SEP'], data_config['table_name']

    idx = 1
    suffix = 'MN'
    fname = f'{PATH}{idx:02d}{SEP}{table_name}{SEP}{suffix}.ftr'
    try:
        df.to_feather(fname)
        print(f"'{fname}' is successfully saved.")
        idx += 1
        
        for key in df_subs.keys():
            suffix = f"MN-SUB{SEP}{key}"
            fname = f'{PATH}{idx:02d}{SEP}{table_name}{SEP}{suffix}.ftr'
            try:
                df_subs[key].reset_index().to_feather(fname)
                idx += 1
                print(f"'{fname}' is successfully saved.")
            except:
                print(f'Fail to save the {fname}')
    except:
        print(f"Fail to save the {fname}")