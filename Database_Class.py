import psycopg2

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
    create_table(table_name, columns, dtypes, id_included=False):
        Creates a new table in the connected database
    insert_rows(table_name, columns, entries):
        Inserts rows into specified SQL table
    query(table_name, row_id=None, row_number=10):
        Fetches first 10 rows of table by default, or can retrieve a specific ID in table
    update_row(table_name, row_id, columns, row_entry):
        Updates specified row with new data from row_entry
    drop_row(table_name, row_id):
        Deletes the row mentioned from table passed in
    drop_table(table_name):
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

    def query(self, row_id=None, row_number=10):
        """
        Fetches first 10 rows of table by default, or can retrieve a specific ID in table

        :param row_id: ID of row to be retrieved
        :type row_id: int
        :param row_number: Number of rows to be retrieved
        :type row_number: int
        :return: None
        """
        # If no row_id given, will return first 10 rows by default
        if row_id is None:
            print(f'First 10 rows of {self.table_name}: \n')
            self.cursor.execute(f"SELECT * FROM {self.table_name} LIMIT {row_number}")
            table = self.cursor.fetchall()
            for row in table:
                print(f'{row} \n')
        else:
            self.cursor.execute(f"SELECT * FROM {self.table_name} WHERE id={row_id}")
            row = self.cursor.fetchall()
            print(f'Row {row_id} data: \n{row[0]}')

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

    def drop_row(self, row_id):
        """
        Deletes the row mentioned from table passed in

        :param row_id: ID of row to be dropped
        :type row_id: int
        :return: None
        """
        drop_row_command = f"DELETE FROM {self.table_name} WHERE id={row_id}"
        self.cursor.execute(drop_row_command)
        print(f'Row with id = "{row_id}" successfully deleted')

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
