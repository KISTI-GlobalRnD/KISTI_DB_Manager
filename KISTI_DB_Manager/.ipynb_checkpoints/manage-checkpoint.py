# KISTI_DB_Manager/manage.py
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
# Database configuration
db_config = {
    'host': Host,  # Update as needed
    'user': User,       # Update as needed
    'password': Password,       # Update as needed
    'database': DB  # Update as needed
}

# Table name
table_name = 'example_table'

# Generate and execute CREATE TABLE SQL
create_table(f, table_name, db_config)
fill_table_from_file(f, table_name, db_config)

"""
import pymysql
from pymysql.err import ProgrammingError
import pandas as pd

__all__ = ["is_Null", "init_MySQL"]


def is_Null(_type, _null_ratio):
    """Return about Null part for SQL query """
    if _null_ratio == 0:
        _type += ' NOT NULL'
    return _type


def read_Description(data_config):
    """Read the Description File"""
    PATH, f = data_config['PATH'], data_config['file_name']
    df_res = pd.read_csv(f'{PATH}Desc_{f[:-4]}.csv', index_col=0)
    return df_res


def generate_create_table_sql(data_config):
    """Generate SQL statement for creating a table based on DataFrame dtypes."""
    df_desc = read_Description(data_config)
    column_types = {idx: df_desc.loc[idx, 'Type'] for idx in df_desc.index}
    for idx in df_desc.index:
        _null_ratio = df_desc.loc[idx, 'Null_ratio']
        column_types[idx] = is_Null(column_types[idx], _null_ratio)
    columns = [f"`{col}` {dtype}" for col, dtype in column_types.items()]
    columns_str = ", ".join(columns)
    return f"CREATE TABLE `{data_config['table_name']}` ({columns_str});"


def create_table(data_config, db_config):
    """
    Creates a table in a MariaDB database based on a DataFrame structure.
    """
    from pymysql import Error
    
    # Generate CREATE TABLE SQL statement
    create_table_sql = generate_create_table_sql(data_config)
    
    try:
        # Connect to MariaDB
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        
        # Execute CREATE TABLE SQL statement
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table `{data_config['table_name']}` created successfully.")
    except Error as e:
        print(f"Error: {e}")
    finally:
        pass
        # if conn:
        #     cursor.close()
        #     conn.close()

def convert_datetime(df, data_config, FORMAT='%Y-%m-%d %H:%M:%S'):
    # read df_desc
    df_desc = read_Description(data_config)
    # FORMAT = '%Y-%m-%d %H:%M:%S'
    msk = df_desc.Type == 'DATETIME'
    cols = df_desc[msk].index
    for col in cols:
        # 날짜 포맷을 MySQL이 인식할 수 있는 형식으로 변환
        df[col] = pd.to_datetime(df[col]).dt.strftime(FORMAT)
    return df

def fill_table_from_file(data_config, db_config, sep='__'):
    from sqlalchemy import create_engine
    from .preview import read_data_from_tabular
    PATH, f, table_name, TYPE = data_config['PATH'], data_config['file_name'], data_config['table_name'], data_config['file_type']
    Conv_DATETIME = data_config['Conv_DATETIME']
    
    # Read the file into a DataFrame
    df = read_data_from_tabular(data_config)
    
    # dot to underscore
    df.columns = [x.replace('.', sep) for x in df.columns]
    
    # DateTime Convert
    if Conv_DATETIME:
        df = convert_datetime(df, data_config)

    # Create a SQLAlchemy engine for the database connection
    db_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
    engine = create_engine(db_url)

    # Use pandas to_sql() function to insert data into the table
    # Replace 'append' with 'replace' if you want to overwrite existing data in the table
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Data inserted into table `{table_name}` successfully.")


def drop_table(table_name, db_config):
    """
    Drops a specified table from the database.

    Parameters:
    db_config (dict): A dictionary containing the database connection parameters.
    table_name (str): The name of the table to be dropped.
    """
    try:
        # Establish connection to the database
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # Prepare the DROP TABLE statement
        drop_query = f"DROP TABLE IF EXISTS `{table_name}`;"

        # Execute the DROP TABLE statement
        cursor.execute(drop_query)
        conn.commit()

        print(f"Table `{table_name}` dropped successfully.")

    except ProgrammingError as e:
        print(f"An error occurred while dropping the table: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Ensure the connection is closed even if an error occurs
        if conn:
            cursor.close()
            conn.close()


def drop_DB(database_name, db_config):
    """
    Drops a specified database in MariaDB or MySQL using pymysql.

    Parameters:
    db_config (dict): A dictionary containing the database server connection parameters.
    database_name (str): The name of the database to be dropped.
    """
    try:
        # Connect to the database server
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # Execute the DROP DATABASE SQL statement
        cursor.execute(f"DROP DATABASE IF EXISTS `{database_name}`;")
        print(f"Database `{database_name}` dropped successfully.")
        cursor.close()
        conn.close()

    except pymysql.Error as e:
        print(f"Failed to drop database `{database_name}`. Error: {e}")
    # finally:
    #     # Ensure the connection is closed
    #     if conn:
    #         cursor.close()
    #         conn.close()


def create_DB(DB_name, CHARACTER_SET, COLLATE, db_config):
    """
    Create a specified table from the database.

    Parameters:
    db_config (dict): A dictionary containing the database connection parameters.
    table_name (str): The name of the table to be dropped.
    """
    try:
        # Establish connection to the database
        conn = pymysql.connect(host=db_config.get('host'),
                               user=db_config.get('user'),
                               password=db_config.get('password'))
        cursor = conn.cursor()

        # Prepare the DROP TABLE statement
        query = f"CREATE DATABASE IF NOT EXISTS `{DB_name}` CHARACTER SET {CHARACTER_SET} COLLATE {COLLATE};"

        # Execute the DROP TABLE statement
        cursor.execute(query)
        conn.commit()

        print(f"Database `{DB_name}` created successfully.")

    except pymysql.Error as e:
        print(f"Failed to create database `{DB_name}`. Error: {e}")
    finally:
        # Ensure the connection is closed even if an error occurs
        if conn:
            cursor.close()
            conn.close()


def backup_database_subprocess(db_config, data_config):
    """
    Backs up a MariaDB/MySQL database using mysqldump and a db_config dictionary.

    Parameters:
    db_config (dict): A dictionary containing the database connection parameters.
                      Expected keys: 'host', 'user', 'password', 'database'.
    output_file (str): Path to the output file where the backup will be saved.
    Note:
    Using subprocess allows for better handling of the command's stdout and stderr, and it can avoid shell injection vulnerabilities
    """
    import subprocess

    output_file = f'{data_config["out_path"]}{db_config["database"]}.sql'
    command = [
        "mysqldump",
        f"-h{db_config.get('host')}",
        f"-u{db_config.get('user')}",
        f"--password={db_config.get('password')}",
        db_config.get('database')
    ]
    with open(output_file, 'w') as f:
        subprocess.run(command, stdout=f)


def set_index(db_config, data_config):
    """Set Index using Description File"""
    try:
        # Connect to the database server
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        df_desc = read_Description(data_config)
        table_name = data_config['table_name']
        for col in df_desc.index:
            if df_desc.loc[col, 'is_key']:
                _sql = f'CREATE INDEX `IDX_{table_name.upper()}_{col.upper()}` ON `{table_name}` ({col});'
                # Execute the CREATE INDEX SQL statement
                cursor.execute(_sql)
                print(f"Set Index the `{col}` on `{table_name}` successfully.")

    except pymysql.Error as e:
        print(f"Failed to create index `{table_name}`. Error: {e} with {_sql}")
    finally:
        # Ensure the connection is closed
        if conn:
            cursor.close()
            conn.close()


def optimize_table(db_config, data_config):
    """Optimize the table for MariaDB"""
    try:
        # Connect to the database server
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        table_name = data_config['table_name']
        _sql = f'OPTIMIZE TABLE `{table_name}`;'
        # Execute the CREATE INDEX SQL statement
        cursor.execute(_sql)
        print(f"Optimize table `{table_name}` successfully.")

    except pymysql.Error as e:
        print(f"Failed to optimize table `{table_name}`. Error: {e}")
    finally:
        # Ensure the connection is closed
        if conn:
            cursor.close()
            conn.close()

def init_MySQL():
    """ Initializing"""
    import os
    os.system('service mysql start')
