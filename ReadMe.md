# Summary

Made by Young Jin Kim (kimyoungjin06@gmail.com)
Birth: 2023.12.14
Last Update: 2023.01.17, YJ Kim

Disambiguate Organizations (or Companies, etc) and Authors.
Start by analyzing the bibliography or patent.

# Introduction

This collection of functions and dictionaries provides a comprehensive system for analyzing the data in a pandas DataFrame and determining the most suitable SQL data types for storing this data in a MariaDB database. It covers a wide range of data types, including various numeric types, booleans, datetimes, and textual data, with considerations for data range, precision, and special cases. The use of an `Extra_ratio` adds a layer of flexibility, helping to ensure that the chosen data types are robust enough to handle the data without being excessively large. This system is especially useful for dynamically generating SQL table schemas based on the content of a DataFrame.

# Example

Follow 1.DB_Importing.ipynb.


