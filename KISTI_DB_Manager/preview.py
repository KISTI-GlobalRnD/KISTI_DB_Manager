# KISTI_DB_Manager/preview.py
"""
Note
----
Made by Young Jin Kim (kimyoungjin06@gmail.com)
Last Update: 2024.02.05, YJ Kim

MariaDB/MySQL Handling for All type DB
To preprocess, import, export and manage the DB


Now Contains ...

Example
-------
Extra_ratio = 1.2
Min_Year = 1900
Max_Year = 2100
PATH = '../Data/Mobility/CWTS_202401/Footprint data/'
for f in tqdm(flist):
    df = pd.read_csv(f'{PWD}{f}', sep=SEP)
    df_res = pd.DataFrame([])
    for i, col in enumerate(df.columns):
        _series = df[col]
        res = get_Table_Description(_series)
        df_res[col] = res
    df_res.T.to_csv(f'{PATH}Desc_{f[:-4]}.csv')
    # break

"""
import numpy as np
import pandas as pd

Integer_Types = {
    'TINYINT': 1,
    'SMALLINT': 2,
    'MEDIUMINT': 3,
    'INT': 4,
    'BIGINT': 8,
}
Float_Types = {
    'FLOAT': np.float32,
    'DOUBLE': np.float64,
}

get_int_max = lambda x: 2**(int(8*x)-1) -1
get_float_range = lambda x: (np.finfo(x).min, np.finfo(x).max)


def size_to_next_power_of_2(max_length):
    """
    Determines the size as 2^n for storing the longest item in the iterable,
    where n is the smallest integer such that 2^n >= max(len(x)) for x in iterable.

    Parameters:
    max_length (integer): An iterable of items with length (e.g., list of strings).

    Returns:
    int: The calculated size as a power of 2.
    """
    import math
    
    # Calculate the smallest power of 2 greater than or equal to max_length
    power = math.ceil(math.log(max_length, 2))
    size = 2 ** power

    return size
    

def check_range(x, _type_dict, Extra_ratio=1.2):
    """
    Purpose:
        Iterates through potential data types (integer or floating-point)
        and checks if the data in a column fits within their respective ranges.
    Functionality:
        Considers an Extra_ratio to provide flexibility in type selection
        and handles special cases for boolean detection.
    Usage:
        Central function in determining the appropriate SQL data type
        for a column based on its value range.

    Parameters
    ----------
    x : Series
    _type_dict : Dict
    Extra_ratio : Float [1,)
    """
    _not_yet = True
    for _type in _type_dict:
        _byte = _type_dict[_type]
        _max, _min = x.max(), x.min()
        if len(_type_dict) == len(Integer_Types):
            _max_range = get_int_max(_byte)
            if _min >= 0: # Unsigned
                _max_range *= 2
                _type += ' UNSIGNED'
            if (_min == 0) & (_max == 1):
                break # Escape for Boolean
        else:
            _min_range, _max_range = get_float_range(_byte)
            # _max, _min = _max, np.log10(_min)
            # Check if it fits within FLOAT precision
            if (_min_range <= _min <= _max_range) and \
               (_min_range <= _max <= _max_range):
                break
        if _max_range > _max*Extra_ratio:
            _not_yet = False
            break
    return _type, _min, _max, _max_range, _not_yet


def get_MariaDB_Type(_series, Extra_ratio=1.5, Min_Year=1900, Max_Year=2100):
    """
        Backlog.1: Datetime to Year?
        Backlog.2: TEXT vs BLOB
    Purpose:
        The main function to determine the most appropriate MariaDB data type for each column in a DataFrame.
    Functionality:
        Identifies whether a column contains numeric (integer or float), boolean, datetime, or textual data.
        Uses check_range to find the best fitting numeric type.
        Checks for boolean-like data and datetime values.
        Defaults to VARCHAR with a length based on the longest string in the column, adjusted by Extra_ratio.
    Usage:
        Call this function with a pandas series (column) to get the suggested MariaDB data type
        along with additional information like minimum, maximum, and range.
    """
    _type, _min, _max, _max_range, _not_yet = 'Unknown', 'Unknown', 'Unknown', 'Unknown', True
    # Pass Numeric(Int or Float)
    if pd.to_numeric(_series, errors='coerce').notna().all(): 
        try: # When already _series is a datetime, it falling to numeric (can converted)
            if np.issubdtype(_series, np.datetime64):
                _x = pd.to_datetime(_series)
                _type, _min, _max, _max_range, _not_yet = "DATETIME", _x.min(), _x.max(), '-', False
            elif (_series.astype(int) % 1 == 0).all():  # Data is integer
                _type, _min, _max, _max_range, _not_yet = check_range(_series.astype(int), Integer_Types, Extra_ratio)
                # [Deprecated] Special Case for Year type
                if (_min > Min_Year) & (_max < Max_Year):
                    _type, _min, _max, _max_range, _not_yet = 'YEAR', _min, _max, Max_Year, False
            else: # Float
                _type, _min, _max, _max_range, _not_yet = check_range(_series, Float_Types, Extra_ratio)
        except:
            pass
    if _not_yet:
        # Boolean
        if _series.dropna().isin([0, 1, True, False]).all():
            _type, _min, _max, _max_range, _not_yet = 'BOOLEAN', 0, 1, 1, False
        if _not_yet:
            try: # For DateTime
                # Use Try to get speed
                import warnings
                warnings.filterwarnings(action='ignore')
                _x = pd.to_datetime(_series)
                # print(_series)
                _type, _min, _max, _max_range, _not_yet = "DATETIME", _x.min(), _x.max(), '-', False
                warnings.filterwarnings(action='default')
            except: # Fallback to VARCHAR, with length based on the longest string
                _len = _series.astype(str).map(len)
                _max = _len.max()
                _max_range = int(_max*Extra_ratio)
                _type = size_to_next_power_of_2(_max_range)
                if _type > 64:
                    _type = 'TEXT'
                else:
                    _type = f"VARCHAR({_type})"
                _min, _max, _max_range, _not_yet = _len.min(), _max, _max_range, False
                
    _items = ['Type', 'min', 'max', '_max_range', 'Failed']
    _values = [_type, _min, _max, _max_range, _not_yet]
    _res = pd.Series({}, index=_items)
    for i, _item in enumerate(_items):
        _res[_item] = _values[i]
    return _res


def get_Field_Description(_series, Extra_ratio=1.5, Min_Year=1900, Max_Year=2100, unique_ratio_th=.5, freq_ratio_th=1e-3):
    """

    Returns
    -------
        attributes
        Description
        Type
        Example
        Coverage(%)
        min
        max
        mean
        std
        uniq
        freq
        enter
    """
    _items = ['Description', 'Type', 'Example', 'Coverage', 'min', 'max', 'mean', 'std', 'top', 'uniq', 'freq', 'entr', 'Failed']
    _res = pd.Series({}, index=_items, name='attributes')
    _desc = _series.describe()
    for _item in _items:
        try:
            _res[_item] = _desc[_item]
        except:
            pass
    _desc2 = _series.astype('str').describe()
    
    _res2 = get_MariaDB_Type(_series.dropna(), Extra_ratio, Min_Year, Max_Year)
    for _item in _res2.index:
        try:
            _res[_item] = _res2[_item]
        except:
            pass

    _res['Description'] = ''
    try:
        _res['Example'] = _series.dropna().reset_index(drop=True)[0]
    except:
        _res['Example'] = ''
    _res['Coverage'] = _desc['count'] / _series.size
    _res['uniq'] = len(set(_series))
    _res['uniq_ratio'] = _res['uniq'] / _series.size
    _res['freq'] = _desc2['freq'] / _series.size
    _res['entr'] = calculate_shannon_entropy(_series)
    _res['Null_ratio'] = 0.01
    _res['is_key'] = key_selection(_res, unique_ratio_th, freq_ratio_th)
    return _res


def calculate_shannon_entropy(series):
    """
    Calculate the Shannon Entropy.
    """
    value_counts = series.value_counts()
    probabilities = value_counts / len(series)
    entropy = -np.sum(probabilities * np.log2(probabilities))
    return entropy


def key_selection(_res, unique_ratio_th, freq_ratio_th):
    _msk1 = _res['Coverage'] == 1.
    _msk2 = _res['uniq_ratio'] > unique_ratio_th
    _msk3 = _res['freq'] < freq_ratio_th
    _msk4 = _res['Type'] != 'TEXT'
    _msk = _msk1 & _msk2 & _msk3 & _msk4
    return _msk


def update_data_config(f, data_config):
    data_config['file_name'] = f
    data_config['table_name'] = f[:-4]
    data_config['file_type'] = f[-3:]
    return data_config


def get_Table_Description(data_config, params, verbose=False, sep='__'):
    PATH, f, SEP, Conv_DATETIME = data_config['PATH'], data_config['file_name'], data_config['SEP'], data_config['Conv_DATETIME']
    df = read_data_from_tabular(data_config)
    df_desc = pd.DataFrame([])
    for i, col in enumerate(df.columns):
        _series = df[col]
        if verbose:
            print(col)
        res = get_Field_Description(_series, **params)
        if (res['Type'] == 'DATETIME') & (Conv_DATETIME == False):
            res['Type'] = 'VARCHAR(64)' # prefix
        df_desc[col] = res

    # dot to underscore
    df_desc.columns = [x.replace('.', sep) for x in df_desc.columns]
    df_desc.T.to_csv(f"{PATH}Desc_{data_config['table_name']}.csv")
    print(f"Generate the Description file for table `{data_config['table_name']}`")
    return df_desc.T


def read_data_from_tabular(data_config):
    """Read the tabular Data File"""
    PATH, f, TYPE = data_config['PATH'], data_config['file_name'], data_config['file_type']
    if (TYPE == 'csv') | (TYPE == 'txt'):
        SEP = data_config['SEP']
        df = pd.read_csv(f'{PATH}{f}', sep=SEP)
    elif TYPE == 'ftr':
        df = pd.read_feather(f'{PATH}{f}')
    return df