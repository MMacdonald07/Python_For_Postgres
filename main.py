"""
A program which reads in a .csv file and makes use of the psycopg2 library to establish a connection to PostgreSQL,
providing various SQL database handling functions which are automated by Python.

Author: Matthew MacDonald
"""
import pandas as pd
import numpy as np

from Database_Class import DatabaseConnection


def sql_type_converter(dtype):
    """
    Takes a python data type and returns the equivalent SQL data type name

    :param dtype: data type for given column
    :return: SQL version of Python data type
    :rtype: str
    """
    types = {
        np.float64: 'real',
        str: 'varchar(200)',
        np.int64: 'int',
        bool: 'bit',
    }
    new_type = types[dtype]
    return new_type


def null_filler(df, cols):
    """
    Fills all None values in database to line up with float "nan"

    :param df: DataFrame containing all values to be inserted into SQL table
    :type df: DataFrame
    :param cols: List of all columns present in table_name
    :type cols: list
    :return: Adjusted DataFrame
    :rtype: DataFrame
    """
    for i in range(len(cols)):
        if type(df[cols[i]].iloc[0]) == str:
            df[cols[i]].fillna('nan', inplace=True)
    return df


if __name__ == '__main__':
    path = '-----'
    data = pd.read_csv(path)
    data = data.copy()
    table_name = 'players'

    columns = [column for column in data.columns]
    data = null_filler(data, columns)
    # Scans through DataFrame and determines the equivalent SQL data type of each column
    dtypes = [sql_type_converter(type(entry)) for entry in data.iloc[0]]
    # Stores all values from file in a tuple for database insertion
    entries = [row[1:] for row in data.itertuples()]

    # New row data for the update_row method
    new_path = '-----'
    new_data = pd.read_csv(new_path)
    new_data = new_data.copy()
    replaced_entry = [row[1:] for row in new_data.itertuples()]

    database_connection = DatabaseConnection(table_name)
    database_connection.create_table(columns, dtypes)
    database_connection.insert_rows(columns, entries)
    database_connection.update_row(columns, replaced_entry, row_id=1)
    database_connection.query()
    database_connection.drop_row(row_id=1)
    database_connection.drop_table()
    database_connection.close_connection()
