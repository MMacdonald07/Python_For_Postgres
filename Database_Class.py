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
    query(columns, order=None, row_id=None, row_number=None, conditions=None):
        Fetches all rows of table in ascending order by default, or can retrieve a specific ID in table
    update_rows(columns, row_entries, conditions=None):
        Updates specified row with new data from row_entry
    rename_table(new_table_name):
        Renames whole table
    add_columns(new_columns, new_dtypes):
        Adds new columns to table
    rename_columns(old_column_names, new_column_names):
        Renames specified columns in table
    drop_columns(column_names):
        Drops list of columns from table
    delete_rows(conditions=None):
        Deletes listed rows from table
    drop_table():
        Drops whole table
    close_connection():
        Closes connection to cursor and database
    equal(column_name, values):
        Creates equivalent condition strings in a SQL format
    greater_than(column_name, value):
        Creates greater than condition strings in a SQL format
    less_than(column_name, value):
        Creates less than condition strings in a SQL format
    between(column_name, start_value, end_value):
        Creates between condition strings in a SQL format
    not_equal(column_name, value):
        Creates not equal to condition strings in a SQL format
    not_null(column_name):
        Creates condition strings in a SQL format to find all non-Null values of a column
    asc(column_name):
        Creates ascending order by strings in a SQL format
    desc(column_name):
        Creates descending order by strings in a SQL format
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
        if len(entries) == 0:
            print('No data to insert')
            return
        # Formats columns list to return a bracketed list without quotations around each column name
        formatted_cols = "(" + "{0}".format(', '.join(map(str, columns))) + ")"
        insert_command = f"INSERT INTO {self.table_name} {formatted_cols} VALUES %s"
        # Uses execute_values to insert all data at once, only making a single commit
        execute_values(self.cursor, insert_command, entries)
        print(str(len(entries)) + ' records successfully inserted into database')

    def query(self, columns, conditions=None, order=None, row_number=None):
        """
        Fetches all table rows by default, or can retrieve a specific ID in table, returning in DataFrame form

        :param conditions: Conditions to be included in WHERE statement for overall query
        :param columns: Columns present in table_name
        :type columns: list
        :param order: Either ascending or descending order
        :type order: str
        :param row_number: Number of rows to be retrieved
        :type row_number: int
        :return: DataFrame of fetch results
        :rtype: pd.DataFrame
        """
        # If no row_number or conditions given, will return all rows by default
        if conditions is None:
            if row_number is not None:
                self.cursor.execute(f"SELECT * FROM {self.table_name} LIMIT {row_number} ")
            else:
                self.cursor.execute(f"SELECT * FROM {self.table_name} ")
            result = self.cursor.fetchall()
            df = pd.DataFrame(result, columns=columns)
            return df
        else:
            query_command = f" SELECT * FROM {self.table_name} WHERE "
            # If given as a single condition str, this will be converted to a list
            if type(conditions) == str:
                conditions = [conditions]
            if len(conditions) > 1:
                for i in range(len(conditions) - 1):
                    query_command += f"{conditions[i]} AND "
                query_command += conditions[-1]
            else:
                query_command += conditions[0]
        if order is not None:
            query_command += f" ORDER BY {order}"
        self.cursor.execute(query_command)
        result = self.cursor.fetchall()
        df = pd.DataFrame(result, columns=columns)
        return df

    def update_rows(self, columns, row_entries, conditions=None):
        """
        Updates specified row with new data from row_entry

        :param conditions: Conditions to be included in WHERE statement for overall query
        :param columns: Columns present in table_name
        :type columns: list
        :param row_entries: List of values to be inserted to update row_id
        :type row_entries: list
        :return: None
        """
        if conditions is None:
            print('No conditions given, update cannot be completed')
        elif len(row_entries) == 0:
            print('No new rows given, update cannot be completed')
        else:
            update_command = f"UPDATE {self.table_name} SET "
            for i in range(len(columns) - 1):
                update_command += f"{columns[i]} = '{row_entries[0][i]}', "
            update_command += f"{columns[-1]} = '{row_entries[0][-1]}' WHERE "
            if type(conditions) == str:
                conditions = [conditions]
            if len(conditions) > 1:
                for i in range(len(conditions) - 1):
                    update_command += f"{conditions[i]} AND "
                update_command += conditions[-1]
            else:
                update_command += conditions[0]
            self.cursor.execute(update_command)
            print(f'Rows of table "{self.table_name}" have been updated')

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
        if type(new_columns) == str:
            new_columns = [new_columns]
        if type(new_dtypes) == str:
            new_dtypes = [new_dtypes]
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
        if type(old_column_names) == str:
            old_column_names = [old_column_names]
        if type(new_column_names) == str:
            new_column_names = [new_column_names]
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
            print('All names the same, no changes made')
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
        if type(column_names) == str:
            column_names = [column_names]
        if len(column_names) > 1:
            for i in range(len(column_names) - 1):
                drop_column_command += f"{column_names[i]}, "
            drop_column_command += f"{column_names[-1]}"
        else:
            drop_column_command += f"{column_names[0]}"
        self.cursor.execute(drop_column_command)
        print(f'Columns successfully dropped from "{self.table_name}"')

    def delete_rows(self, conditions=None):
        """
        Deletes the list of rows from table_name which satisfy the given conditions

        :param conditions: Conditions to be included in WHERE statement for overall query
        :return: None
        """
        delete_row_command = f"DELETE FROM {self.table_name} "
        if conditions is None:
            # Will delete all rows if no conditions given
            self.cursor.execute(delete_row_command)
        else:
            delete_row_command += "WHERE "
            if type(conditions) == str:
                conditions = [conditions]
            if len(conditions) > 1:
                for i in range(len(conditions) - 1):
                    delete_row_command += f"{conditions[i]} AND "
                delete_row_command += conditions[-1]
            else:
                delete_row_command += conditions[0]
        self.cursor.execute(delete_row_command)
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

    @staticmethod
    def equal(column_name, values):
        """
        Function used to create equivalent condition strings in a SQL format.

        :param column_name: Name of column inputted
        :type column_name: str
        :param values: Values to be equated to
        :return: "column_name" = 'value'
        :rtype: str
        """
        # if not a list of conditions to check
        if (type(values) != list) and (type(values) != tuple):
            condition = f" '{column_name}' = '{values}' "
        else:
            condition = '( '
            for i in range(len(values)):
                condition += f" '{column_name}' = '{values[i]}' "
                if i != (len(values) - 1):
                    condition += "OR "
            condition += " )"
        return condition

    @staticmethod
    def greater_than(column_name, value):
        """
        Function used to create greater than condition strings in a SQL format.

        :param column_name: Name of column inputted
        :type column_name: str
        :param value: Value to be compared to
        :type value: int
        :return: "column_name" > 'value'
        :rtype: str
        """
        condition = f" '{column_name}' > '{value}' "
        return condition

    @staticmethod
    def less_than(column_name, value):
        """
        Function used to create less than condition strings in a SQL format.

        :param column_name: Name of column inputted
        :type column_name: str
        :param value: Value to be compared to
        :type value: int
        :return: "column_name" less than 'value'
        :rtype: str
        """
        condition = f" '{column_name}' < '{value}' "
        return condition

    @staticmethod
    def between(column_name, start_value, end_value):
        """
        Function used to create between condition strings in a SQL format

        :param column_name: Name of column inputted
        :type column_name: str
        :param start_value: Lower end of the region.
        :type start_value: int
        :param end_value: Higher end of the region.
        :type end_value: int
        :return: "column_name" > 'start_value' AND "column_name" less than 'end_value'
        :rtype: str
        """
        condition = f" '{column_name}' > '{start_value}' AND '{column_name}' < '{end_value}' "
        return condition

    @staticmethod
    def not_equal(column_name, value):
        """
        Function used to create non-equivalent condition strings in a SQL format.

        :param column_name: Name of column inputted
        :type column_name: str
        :param value: Value to be compared to
        :return: "column_name" != 'value'
        :rtype: str
        """
        condition = f" '{column_name}' != '{value}' "
        return condition

    @staticmethod
    def not_null(column_name):
        """
        Function used to create not null condition strings in a SQL format.

        :param column_name: Name of column inputted
        :type column_name: str
        :return: "column_name" is not null
        :rtype: str
        """
        condition = f" '{column_name}' is not null "
        return condition

    @staticmethod
    def asc(column_name):
        """
        Function used to create ascending order by strings in a SQL format.

        :param column_name: The column the user wants to order
        :type column_name: str
        :return: "column_name ASC"
        :rtype: str
        """
        return f" '{column_name}' ASC"

    @staticmethod
    def desc(column_name):
        """
        Function used to create descending order by strings in a SQL format.

        :param column_name: The column the user wants to order
        :type column_name: str
        :return: "column_name DESC"
        :rtype: str
        """
        return f" '{column_name}' DESC"
