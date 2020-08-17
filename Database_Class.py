import psycopg2
import pandas as pd

from psycopg2.extras import execute_values
from config import user, password, dbname


class DatabaseConnection:
    """
    Class that represents a PostgreSQL database

    ...

    Attributes
    -------
    table_name: str
        Name of SQL table

    Methods
    -------
    create_table(columns, dtypes, id_included=False):
        Creates a new table in the connected database
    insert_rows(columns, entries):
        Inserts rows into specified SQL table
    query(columns, order='asc', row_id=None, row_number=None):
        Fetches all rows of table by default, or can retrieve a specific ID in table
    update_row(row_id, columns, row_entry):
        Updates specified row with new data from row_entry
    rename_table(new_table_name):
        Renames whole table
    add_columns(new_columns, new_dtypes):
        Adds new columns to table
    rename_columns(old_column_names, new_column_names):
        Renames specified columns in table
    drop_columns(column_names):
        Drops list of columns from table
    drop_rows(row_ids):
        Deletes listed rows from table
    drop_table():
        Drops whole table
    close_connection():
        Closes connection to cursor and database
    """

    def __init__(self, table_name):
        self.table_name = table_name
        try:
            # Establishes connection to PostgreSQL database
            self.connection = psycopg2.connect(
                dbname=dbname, user=user, host='localhost', password=password, port='5432')
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print(f'Connected to database "{dbname}" on user "{user}"')
        except:
            print(f'Unable to connect to database {dbname}')

    def create_table(self, columns, dtypes, id_included=False):
        """
        Creates a new SQL table in the connected database

        :param columns: Columns present in table_name
        :type columns: list
        :param dtypes: Data types of each column in table_name
        :type dtypes: list
        :param id_included: Whether or not the table comes with its own ID values for each row
        :type id_included: bool
        :return: None
        """
        if not id_included:
            # Will produce signature ID column if none included
            create_table_command = f"CREATE TABLE {self.table_name} (id bigserial PRIMARY KEY, "
            # Concatenates each column name with its SQL data type to produce create table query
            for i in range(len(columns) - 1):
                create_table_command += f"{columns[i]} {dtypes[i]}, "
        else:
            create_table_command = f"CREATE TABLE {self.table_name} ("
            for i in range(len(columns) - 1):
                create_table_command += f"{columns[i]} {dtypes[i]}, "
        create_table_command += f"{columns[-1]} {dtypes[-1]})"
        self.cursor.execute(create_table_command)

        print(f'Table "{self.table_name}" successfully created')

    def insert_rows(self, columns, entries):
        """
        Inserts new rows into given SQL table

        :param columns: Columns present in table_name
        :type columns: list
        :param entries: List of all values to be inserted into table_name
        :type entries: list
        :return: None
        """
        # Formats columns list to return a bracketed list without quotations around each column name
        formatted_cols = "(" + "{0}".format(', '.join(map(str, columns))) + ")"
        insert_command = f"INSERT INTO {self.table_name} {formatted_cols} VALUES %s"
        # Uses execute_values to insert all data at once, only making a single commit
        execute_values(self.cursor, insert_command, entries)
        print(str(len(entries)) + ' records successfully inserted into database')

    def query(self, columns, order='asc', row_id=None, row_number=None):
        """
        Fetches all table rows by default, or can retrieve a specific ID in table, returning in DataFrame form

        :param columns: Columns present in table_name
        :type columns: list
        :param frame: Whether or not to return the data obtained in a DataFrame
        :type frame: bool
        :param order: Either ascending or descending order
        :type order: str
        :param row_id: ID of row to be retrieved
        :type row_id: int
        :param row_number: Number of rows to be retrieved
        :type row_number: int
        :return: DataFrame of fetch results
        :rtype: pd.DataFrame
        """
        # If no row_id or conditions given, will return first 10 rows by default
        if row_id is None:
            if order.lower() == 'desc':
                order_by = "ORDER BY DESC"
            else:
                order_by = "ORDER BY ASC"
            if row_number is not None:
                self.cursor.execute(f"SELECT * FROM {self.table_name} LIMIT {row_number} {order_by}")
            else:
                self.cursor.execute(f"SELECT * FROM {self.table_name} {order_by}")
            table = self.cursor.fetchall()
            df = pd.DataFrame(table, columns=columns)
            return df
        else:
            self.cursor.execute(f"SELECT * FROM {self.table_name} WHERE id={row_id}")
            row = self.cursor.fetchall()
            df = pd.DataFrame(row, columns=columns)
            return df

    def update_row(self, columns, row_entry, row_id):
        """
        Updates specified row with new data from row_entry

        :param columns: Columns present in table_name
        :type columns: list
        :param row_entry: List of values to be inserted to update row_id
        :type row_entry: list
        :param row_id: ID of row to be updated
        :type row_id: int
        :return: None
        """
        update_command = f"UPDATE {self.table_name} SET "
        for i in range(len(columns) - 1):
            update_command += f"{columns[i]} = '{row_entry[0][i]}', "
        update_command += f"{columns[-1]} = '{row_entry[0][-1]}' WHERE id={row_id}"
        self.cursor.execute(update_command)
        print(f'Row {row_id} of table "{self.table_name}" has been updated')

    def rename_table(self, new_table_name):
        """
        Renames whole table

        :param new_table_name: New name of table

        :return: None
        """
        if new_table_name == self.table_name:
            print("This is already the table's name")
            return

        rename_command = f"ALTER TABLE {self.table_name} RENAME TO {new_table_name}"

        self.cursor.execute(rename_command)
        self.table_name = new_table_name
        print(f'Table renamed to "{new_table_name}"')

    def add_columns(self, new_columns, new_dtypes):
        """
        Adds new columns to table_name

        :param new_dtypes: Data types of each new column in table_name
        :type new_dtypes: list
        :param new_columns: A list of new columns to be added to table_name
        :type new_columns: list
        :return: None
        """
        add_column_command = f"ALTER TABLE {self.table_name} ADD COLUMN "
        if len(new_columns) > 1:
            for i in range(len(new_columns) - 1):
                add_column_command += f"{new_columns[i]} {new_dtypes[i]}, "
            add_column_command += f"{new_columns[-1]} {new_dtypes[-1]})"
        else:
            add_column_command += f"{new_columns[0]} {new_dtypes[0]}"
        self.cursor.execute(add_column_command)
        print(f'New columns added to "{self.table_name}"')

    def rename_columns(self, old_column_names, new_column_names):
        """
        Takes old_column_names as previous columns from table_name to rename

        :param old_column_names: List of all the columns which are to be renamed
        :type old_column_names: list
        :param new_column_names: List containing all new column names
        :type new_column_names: list
        :return: None
        """
        rename_column_command = f"ALTER TABLE {self.table_name} RENAME COLUMN"
        if len(new_column_names) > 1:
            for i in range(len(new_column_names) - 1):
                # Skips rename if new column name same as previous one
                if old_column_names[i] == new_column_names[i]:
                    continue
                else:
                    rename_column_command += f"{old_column_names[i]} TO {new_column_names[i]}, "
            if old_column_names[-1] != new_column_names[-1]:
                rename_column_command += f"{old_column_names[-1]} TO {new_column_names[-1]})"
        else:
            if old_column_names[0] != new_column_names[0]:
                rename_column_command += f"{old_column_names[0]} TO {new_column_names[0]}"

        if rename_column_command == f"ALTER TABLE {self.table_name} RENAME COLUMN":
            # if all names were equivalent
            return

        self.cursor.execute(rename_column_command)
        print('Columns successfully renamed')

    def drop_columns(self, column_names):
        """
        Drops a list of columns from table_name

        :param column_names: The list of columns being dropped
        :type column_names: list
        :return: None
        """
        drop_column_command = f'ALTER TABLE {self.table_name} DROP COLUMN'
        if len(column_names) > 1:
            for i in range(len(column_names) - 1):
                drop_column_command += f"{column_names[i]}, "
            drop_column_command += f"{column_names[-1]}"
        else:
            drop_column_command += f"{column_names[0]}"
        self.cursor.execute(drop_column_command)
        print(f'Columns successfully dropped from "{self.table_name}"')

    def drop_rows(self, row_ids):
        """
        Deletes the list of rows whose IDs are included from table_name passed in

        :param row_ids: IDs of rows to be dropped
        :type row_ids: list
        :return: None
        """
        drop_row_command = f"DELETE FROM {self.table_name} WHERE "
        if len(row_ids) > 1:
            for i in range(len(row_ids) - 1):
                drop_row_command += f"id = {row_ids[i]}, "
            drop_row_command += f"id = {row_ids[-1]}"
        else:
            drop_row_command += f"id = {row_ids[0]}"
        self.cursor.execute(drop_row_command)
        print('Rows successfully deleted')

    def drop_table(self):
        """
        Drops whole table

        :return: None
        """
        drop_table_command = f"DROP TABLE {self.table_name}"
        self.cursor.execute(drop_table_command)
        print(f'Table "{self.table_name}" successfully dropped')

    def close_connection(self):
        """
        Closes program's connection to cursor and database

        :return: None
        """
        self.cursor.close()
        self.connection.close()
        print('PostgreSQL connection is closed')