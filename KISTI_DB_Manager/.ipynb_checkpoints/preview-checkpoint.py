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

__all__ = ["get_MariaDB_Type", "read_data_from_tabular"]

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
    Checks if the data in a column fits within the range of a given data type.
    """
    _not_yet = True
    for _type in _type_dict:
        _byte = _type_dict[_type]
        _max, _min = x.max(), x.min()

        if len(_type_dict) == len(Integer_Types):
            _max_range = get_int_max(_byte)
            if _min >= 0:  # Unsigned
                _max_range *= 2
                _type += ' UNSIGNED'
            if (_min == 0) & (_max == 1):
                break  # Escape for Boolean
        else:
            _min_range, _max_range = get_float_range(_byte)
            # Check if it fits within float precision
            if (_min_range <= _min <= _max_range) and \
               (_min_range <= _max <= _max_range):
                break
        
        # Check if it fits within the allowed range with the Extra_ratio
        if _max_range > _max * Extra_ratio:
            _not_yet = False
            break

    return _type, _min, _max, _max_range, _not_yet


def get_MariaDB_Type(_series, Extra_ratio=1.5, Min_Year=1900, Max_Year=2100):
    """
    Determines the most appropriate MariaDB data type for a given pandas Series.
    """
    _type, _min, _max, _max_range, _not_yet = 'Unknown', 'Unknown', 'Unknown', 'Unknown', True

    # 먼저 문자열이 아닌지 확인하고 문자열일 경우 문자열 처리로 분기
    if _series.dtype == 'object':
        try:
            _len = _series.astype(str).map(len)
            _max = _len.max()
            _max_range = int(_max * Extra_ratio)
            _type = size_to_next_power_of_2(_max_range)
            if _type > 64:
                _type = 'TEXT'
            else:
                _type = f"VARCHAR({_type})"
            _min, _max, _max_range, _not_yet = _len.min(), _max, _max_range, False
        except Exception as e:
            print(f"Error processing series as string: {e}")
        return pd.Series({
            'Type': _type,
            'min': _min,
            'max': _max,
            '_max_range': _max_range,
            'Failed': _not_yet
        })

    # Try to convert to numeric (integer or float)
    numeric_series = pd.to_numeric(_series, errors='coerce')

    if numeric_series.notna().all():
        try:
            # When already a datetime, falling to numeric can be handled
            if np.issubdtype(_series.dtype, np.datetime64):
                _x = pd.to_datetime(_series)
                _type, _min, _max, _max_range, _not_yet = "DATETIME", _x.min(), _x.max(), '-', False
            elif (_series % 1 == 0).all():  # Data is integer (checking if the series consists of whole numbers)
                _type, _min, _max, _max_range, _not_yet = check_range(numeric_series.astype(int), Integer_Types, Extra_ratio)
                if (_min > Min_Year) & (_max < Max_Year):
                    _type, _min, _max, _max_range, _not_yet = 'YEAR', _min, _max, Max_Year, False
            else:  # Data is float
                _type, _min, _max, _max_range, _not_yet = check_range(numeric_series.astype(float), Float_Types, Extra_ratio)
        except Exception as e:
            print(f"Error processing series: {e}")
    
    if _not_yet:
        # Check for boolean
        if _series.dropna().isin([0, 1, True, False]).all():
            _type, _min, _max, _max_range, _not_yet = 'BOOLEAN', 0, 1, 1, False
        if _not_yet:
            try:
                # Try to convert to datetime
                _x = pd.to_datetime(_series)
                _type, _min, _max, _max_range, _not_yet = "DATETIME", _x.min(), _x.max(), '-', False
            except:
                # Fallback to VARCHAR
                _len = _series.astype(str).map(len)
                _max = _len.max()
                _max_range = int(_max * Extra_ratio)
                _type = size_to_next_power_of_2(_max_range)
                if _type > 64:
                    _type = 'TEXT'
                else:
                    _type = f"VARCHAR({_type})"
                _min, _max, _max_range, _not_yet = _len.min(), _max, _max_range, False
    
    return pd.Series({
        'Type': _type,
        'min': _min,
        'max': _max,
        '_max_range': _max_range,
        'Failed': _not_yet
    })


def get_Field_Description(_series, Extra_ratio=1.5, Min_Year=1900, Max_Year=2100, unique_ratio_th=.5, freq_ratio_th=1e-3):
    """
    Returns field description and statistics.
    """
    _items = ['Description', 'Type', 'Example', 'Coverage', 'min', 'max', 'mean', 'std', 'top', 'uniq', 'freq', 'entr', 'Failed']
    _res = pd.Series({item: None for item in _items}, index=_items, name='attributes')  # Initialize with None to avoid dtype issues
    _desc = _series.describe()
    
    for _item in _items:
        try:
            # Ensure compatible dtype by explicit casting
            _res[_item] = _desc[_item] if _item in _desc else None
        except:
            pass
    _desc2 = _series.astype('str').describe()  # Describe as string
    
    # Get MariaDB type (assumed to be another function)
    _res2 = get_MariaDB_Type(_series.dropna(), Extra_ratio, Min_Year, Max_Year)
    for _item in _res2.index:
        try:
            _res[_item] = _res2[_item]
        except:
            pass

    # Set additional values
    _res['Description'] = ''
    try:
        _res['Example'] = _series.dropna().reset_index(drop=True)[0]
    except:
        _res['Example'] = ''
    
    # Assign coverage, unique counts, and ratios
    _res['Coverage'] = _desc['count'] / _series.size if 'count' in _desc else None
    _res['uniq'] = len(_series.unique())
    _res['uniq_ratio'] = _res['uniq'] / _series.size
    _res['freq'] = _desc2['freq'] / _series.size if 'freq' in _desc2 else None
    _res['entr'] = calculate_shannon_entropy(_series)  # Entropy calculation (assumed to be another function)
    
    # Handle Null ratio and key selection
    _res['Null_ratio'] = _series.isnull().mean()  # Calculate null ratio directly from series
    _res['is_key'] = key_selection(_res, unique_ratio_th, freq_ratio_th)  # Key selection logic (assumed to be another function)
    
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
    _msk1 = _res['Coverage'] > .999
    _msk2 = _res['uniq_ratio'] > unique_ratio_th
    _msk3 = _res['freq'] < freq_ratio_th
    _msk4 = _res['Type'] != 'TEXT'
    _msk = _msk1 & _msk2 & _msk3 & _msk4
    return _msk


def update_data_config(f, data_config):
    data_config['file_name'] = f
    data_config['table_name'] = ".".join(f.split('.')[:-1])
    data_config['file_type'] = f.split('.')[-1]
    try:
        data_config['KEYs'] = list(set(data_config['KEYs'] + [data_config['KEY']]))
    except:
        data_config['KEYs'] = [data_config['KEY']]
    return data_config


def get_Table_Description(data_config, params, verbose=False, sep='__'):
    import numpy
    
    PATH, f, SEP, Conv_DATETIME = data_config['PATH'], data_config['file_name'], data_config['SEP'], data_config['Conv_DATETIME']
    df = read_data_from_tabular(data_config)
    df_desc = pd.DataFrame([])
    for i, col in enumerate(df.columns):
        _series = df[col]
        # 리스트가 포함된 컬럼을 문자열로 변환
        _series = _series.apply(lambda x: str(x) if isinstance(x, numpy.ndarray) else x)
        _series = _series.apply(lambda x: str(x) if isinstance(x, dict) else x)
        if verbose:
            print(col)
        res = get_Field_Description(_series, **params)
        if (res['Type'] == 'DATETIME') & (Conv_DATETIME == False):
            res['Type'] = 'VARCHAR(64)' # prefix
        df_desc[col] = res
        if data_config['KEYs']:
            keys = data_config['KEYs']
            for key in keys:
                df_desc.loc["is_key", key] = True

    # dot to underscore
    df_desc.columns = [x.replace('.', sep) for x in df_desc.columns] # Will be deprecated
    df_desc.T.to_csv(f"{PATH}{data_config['table_name']}_Desc.csv")
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
    elif TYPE == 'parquet':
        df = pd.read_parquet(f'{PATH}{f}')
    return df