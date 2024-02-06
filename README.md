# Summary

Made by Young Jin Kim (kimyoungjin06@gmail.com)
Birth: 2023.02.06
Last Update: 2024.02.05, YJ Kim

MariaDB/MySQL Handling for All type DB
To preprocess, import, export and manage the DB

---

# KISTI_DB_Manager

The `KISTI_DB_Manager` package is a comprehensive toolset designed to streamline the process of managing database operations, including creating databases, generating and optimizing tables, and efficiently managing data for analytical purposes. Focused on the needs of researchers and data analysts working with mobility footprint data, this package offers a robust solution for handling large datasets with ease.

## Features

- **Database Management**: Simplify the creation and dropping of databases with configurable character sets and collations.
- **Table Management**: Automated table creation and optimization based on data analysis.
- **Data Ingestion**: Facilitate the direct insertion of data from tabular files into the database, supporting large datasets.
- **Index Management**: Automatically set indexes on tables to improve query performance.
- **Custom Configuration**: Support for custom data and database configurations, allowing for flexible data management strategies.

## Installation

To install `KISTI_DB_Manager`, ensure you have `pymysql` installed as a prerequisite, then download or clone the package repository from its source. The package can be included in your projects by importing:

```bash
pip install pymysql
# Clone or download the KISTI_DB_Manager package from its repository
```

## Quick Start Guide

Here's how to get started with `KISTI_DB_Manager`:

1. **Configure Database and Data Paths**:

   Define your database configuration and data paths. Customize the parameters as needed for your environment and data structure.

   ```python
   db_config = {
       'host': 'localhost',
       'user': 'your_username',
       'password': 'your_password',
       'database': 'your_database_name'
   }

   data_config = {
       'PATH': 'path_to_your_data/',
       'SEP': '\t',
       'file_name': 'your_file_name.txt',
       'table_name': 'your_table_name',
       'out_path': 'path_for_sql_output/'
   }
   ```

2. **Database and Table Management**:

   Easily manage your databases and tables with a few lines of code. For example, to create a new database and set up tables based on your data files:

   ```python
   from KISTI_DB_Manager import manage, preview

   # Create database
   manage.create_DB(db_config['database'], CHARACTER_SET, COLLATE, db_config)

   # Process data files and create tables
   flist = sorted([x for x in os.listdir(data_config['PATH']) if 'txt' in x])
   for f in flist:
       data_config = preview.update_data_config(f, data_config)
       manage.create_table(data_config, db_config)
       manage.fill_table_from_file(data_config, db_config)
   ```

## Advanced Usage

Refer to the `0.1.Sample_Code.ipynb` notebook for advanced use cases, including detailed data preparation, table optimization techniques, and index management strategies for large datasets.

## Contributing

We welcome contributions to `KISTI_DB_Manager`! Please read through our contribution guidelines for details on how to submit pull requests, report issues, or request new features.

## License

`KISTI_DB_Manager` is licensed under the MIT License. See the LICENSE file for more details.

## Support

For support, questions, or more information about `KISTI_DB_Manager`, please contact us at [contact_information].

---

Remember to replace placeholders (like `[contact_information]`, `your_username`, `your_password`, etc.) with your actual information. This `README.md` provides a foundational structure that you can expand upon based on the specific functionalities and capabilities of your package.