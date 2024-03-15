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

__all__ = ["flatten_dict", "read_a_xml", "flatten_nested_json_with_list", 
           "read_dict_from_zip", "extract_data_from_jsons", "read_dict_from_gz", "json_to_key_pairs"]
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
        excepted[key][index_key] = _id
    return _df, df_subs_add_idx, excepted


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
    import json
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
                extracted_dict = json.load(json_file)
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

    for i, _json in enumerate(tqdm(jsons)):
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
            df_subs_tot[key].append(_df_subs[key])
        for key in except_keys:
            try:
                excepted_tot[key].append(excepted[key])
            except:
                print('err log')
                pass
    df_tot = pd.concat(df_tot)
    for key in df_subs_tot.keys():
        df_subs_tot[key] = pd.concat(df_subs_tot[key])
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
    import json
    
    # Initialize an empty dictionary
    extracted_dict = {}
    # Open the gzip-compressed file
    with gzip.open(gz_path, 'rt', encoding='utf-8') as gz_file:
        # Load the JSON content into a dictionary
        json_strs = gz_file.read().split('\n')
        df_data = pd.DataFrame(json_strs, columns=['raw_str'])
        msk = df_data != ''
        df_data = df_data[msk].dropna()
        df_data['json'] =  df_data['raw_str'].apply(json.loads)

    return df_data['json']


def json_to_key_pairs(json_data, parent='origin', parent_type=None, result=None, sep='__'):
    
    _Types = {
        'l': 'List',
        'lv': 'List of Value',
        'ld': 'List of Dict',
        'vld': 'Value in List of Dict',
        'd': 'Dict',
        'v': 'Value',
    }
    if result is None:
        result = [(_Types['d'], np.nan, parent)]
    
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            child_type = _Types['d'] if isinstance(value, dict) else _Types['lv'] if isinstance(value, list) else _Types['v']
            try:
                if any(isinstance(i, dict) for i in value):
                    child_type = _Types['ld']
            except:
                pass
            result.append((child_type, parent, parent+sep+key))
            json_to_key_pairs(value, parent+sep+key, child_type, result)
    elif isinstance(json_data, list):
        item_type = _Types['ld'] if any(isinstance(i, dict) for i in json_data) else _Types['lv']
        # If the list contains dictionaries, treat each key in those dictionaries
        if item_type == _Types['ld']:
            for item in json_data:
                if isinstance(item, dict):
                    for key in item:
                        if parent_type == _Types['ld']:
                            item_type = _Types['vld']
                        result.append((item_type, parent, parent+sep+key))
                        json_to_key_pairs(item[key], parent+sep+key, item_type, result)
        # For lists of primitives, skip adding them directly but mark the presence of a list
        elif parent_type != _Types['lv']:
            if parent is not None:
                result.append((_Types['l'], parent, parent+sep+_Types['l']))

    res_df = pd.DataFrame(list(set(result)), columns=['type', 'parent', 'branch']).sort_values(['type', 'parent', 'branch'])
    # res_df['depth'] = res_df['branch'].apply(lambda x: len(x.split(sep)))
    return res_df
