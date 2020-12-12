"""
Script that accesses a Postgres database, providing several methods such as
querying, updating and inserting rows as well as creating tables.

Author: Matthew MacDonald
"""
import time
import pandas as pd
import datetime as dt

from Database_Class import DatabaseConnection


def introduction():
    """
    Prints out a welcome message to the user

    :return: None
    """
    current_time = str(dt.datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
    welcome_msg = '\n' + current_time + '\n'
    welcome_msg += 66 * '=' + '\n'
    welcome_msg += 'Welcome to Python for Postgres! \n'
    print(welcome_msg)
    print('To close the program, simply input anything which is not a given option at the choice screens')
    time.sleep(1)


def condition_creator(database_connection):
    """
    Creates the string for a conditional statement based on the user's choices

    :param database_connection: connection to PostgreSQL table
    :type database_connection: DatabaseConnection
    :return: properly formatted SQL condition for WHERE query
    :rtype: str
    """
    comparison_options = ' eq --> Equivalent to \n ' \
                         'gt --> Column value greater than given value \n ' \
                         'lt --> Column value less than given value \n ' \
                         'be --> Between two values \n ne --> Not equal to \n nn --> Non-null values \n'
    print(comparison_options)
    choice = input('Type in the comparator you would like to use:   ')
    column_name = input('What column would you like to compare:     ')
    choice = choice.lower()
    if choice == 'eq':
        values_str = input(
            'What values would you like to use to compare:     ')
        # Allows for multiple values to be inputted in equivalence comparator
        values = [item for item in values_str.replace(',', ' ').split()]
        condition = database_connection.equal(column_name, values)
    elif choice == 'gt':
        value = input('What value would you like to use to compare:     ')
        condition = database_connection.greater_than(column_name, value)
    elif choice == 'lt':
        value = input('What value would you like to use to compare:     ')
        condition = database_connection.less_than(column_name, value)
    elif choice == 'be':
        start_value = input(
            'What start value would you like to use to compare:     ')
        end_value = input(
            'What end value would you like to use to compare:     ')
        condition = database_connection.between(
            column_name, start_value, end_value)
    elif choice == 'ne':
        value = input('What value would you like to use to compare:     ')
        condition = database_connection.not_equal(column_name, value)
    elif choice == 'nn':
        condition = database_connection.not_null(column_name)
    else:
        print(
            f'Sorry, {choice} was not one of the options, now halting the operation...')
        return
    return condition


def order_by_creator(database_connection):
    """
    Creates the string for an order_by statement based on the user's choices

    :param database_connection: connection to PostgreSQL table
    :type database_connection: DatabaseConnection
    :return: properly formatted SQL condition for ORDER BY query
    :rtype: str
    """
    column_name = input(
        'What column would you like to order the data by:     ')
    while True:
        direction = input(
            'Ascending or descending? a for ascending and d for descending:   ')
        if direction.lower() == 'a':
            order = database_connection.asc(column_name)
            break
        elif direction.lower() == 'd':
            order = database_connection.desc(column_name)
            break
        # Ensures user inputs either 'a' or 'd'
        else:
            print('That was not a valid option...')
            continue
    return order


def main():
    """
    Executes the main code of the script

    :return: None
    """
    introduction()
    # Initiates continuous loop
    while True:
        table = input(
            'Please input the name of the table you would like to use:  ')
        # Instantiates DatabaseConnection object
        database_connection = DatabaseConnection(table)
        time.sleep(1)
        options = ' a --> Alter existing table \n ' \
                  'c --> Create new table \n d --> Delete stuff \n i --> Insert rows \n ' \
                  'q --> Query data \n s --> Save the table as a CSV \n u --> Update rows \n'
        print(options)
        user_choice = input(
            'Please select one of the above options for what you would like to do:  ')
        user_choice = user_choice.lower()
        if user_choice == 'a':
            alter_options = ' a --> Add new columns \n rc --> Rename columns \n rt --> Rename whole table \n'
            print(alter_options)
            alter_choice = input(
                'Choose which of these processes you would like to use:    ')
            if alter_choice == 'a':
                new_columns_str = input(
                    'Pass a list of new column names to add:    ')
                # Converts the string variable to a list with each column name an element
                new_columns = [
                    item for item in new_columns_str.replace(',', ' ').split()]
                new_dtypes_str = input(
                    'Pass a list of the pythonic data type for each new column:   ')
                new_dtypes = [
                    item for item in new_dtypes_str.replace(',', ' ').split()]
                database_connection.add_columns(new_columns, new_dtypes)
            elif alter_choice == 'rc':
                old_column_names_str = input(
                    'Pass a list of old column names to change:    ')
                old_column_names = [
                    item for item in old_column_names_str.replace(',', ' ').split()]
                new_column_names_str = input(
                    'Pass a list of new column names to be changed to:   ')
                new_column_names = [
                    item for item in new_column_names_str.replace(',', ' ').split()]
                database_connection.rename_columns(
                    old_column_names, new_column_names)
            elif alter_choice == 'rt':
                new_table_name = input(
                    'What would you like to rename the table to:    ')
                database_connection.rename_table(new_table_name)
            else:
                print(
                    f'Sorry, {alter_choice} was not one of the options, halting operation now...')
        elif user_choice == 'c':
            print('To create a table, a CSV file is needed including columns and rows so the program can '
                  'gather the data type of each column')
            filepath = input('Please type the whole path of this data:    ')
            id_bool = input(
                'Does the CSV file already contain an index? y for yes and anything else for no:    ')
            if id_bool.lower() == 'y':
                id_included = True
            else:
                id_included = False
            try:
                # Reads in DataFrame using pandas
                data = pd.read_csv(filepath)
                data = data.copy()
                # Stores each column name in a list
                columns = [column for column in data.columns]
                database_connection.create_table(columns, data, id_included)
            # If file doesn't exist, issue is raised to user and process stopped
            except FileNotFoundError:
                print('Sorry, that file does not exist, halting operation now...')
        elif user_choice == 'd':
            delete_options = ' c --> Drop columns \n r --> Delete rows \n t --> Drop whole table (Use with caution!) \n'
            print(delete_options)
            delete_choice = input(
                'Choose which of these processes you would like to use:    ')
            if delete_choice == 'c':
                drop_columns_str = input('Pass a list of columns to drop:    ')
                drop_columns = [
                    item for item in drop_columns_str.replace(',', ' ').split()]
                database_connection.drop_columns(drop_columns)
            elif delete_choice == 'r':
                print(
                    'Warning - if no conditions are specified all rows will be deleted by default')
                conditions_choice = input(
                    'Would you like to include conditions? y for yes and n for no:    ')
                if conditions_choice.lower() == 'y':
                    conditions = [condition_creator(database_connection)]
                    while True:
                        multiple_conditions_choice = input('Would you like to include more conditions? '
                                                           'y for yes, anything else for no:    ')
                        # If user wishes to add more conditions, program will continue looping
                        if multiple_conditions_choice.lower() == 'y':
                            conditions.append(
                                condition_creator(database_connection))
                            continue
                        # If user does not want to add more conditions, loop will break
                        else:
                            break
                    # Ensures rows are only deleted when conditions are specified if user has asked for them
                    if conditions is not None:
                        database_connection.delete_rows(conditions)
                elif conditions_choice.lower() == 'n':
                    database_connection.delete_rows()
                else:
                    print(f'{conditions_choice} was not an option!')
            elif delete_choice == 't':
                database_connection.drop_table()
            else:
                print(
                    f'Sorry, {delete_choice} was not one of the options, halting operation now...')
        elif user_choice == 'i':
            print('To insert rows, a CSV file is needed containing the rows to be added')
            filepath = input('Please type the whole path of this data:    ')
            try:
                data = pd.read_csv(filepath)
                data = data.copy()
                columns = [column for column in data.columns]
                database_connection.insert_rows(columns, data)
            except FileNotFoundError:
                print('Sorry, that file does not exist, halting operation now...')
        elif user_choice == 'q':
            number_choice = input(
                'Would you like to limit results? y for yes and n for no:    ')
            if number_choice.lower() == 'y':
                while True:
                    row_number = input(
                        'How many results would you like to have returned:   ')
                    # Ensures the user inputs an integer value for number of rows returned
                    try:
                        row_number = int(row_number)
                        break
                    except ValueError:
                        print('Please only input an integer value')
                        continue
                conditions_choice = input(
                    'Would you like to include conditions? y for yes and n for no:    ')
                if conditions_choice.lower() == 'y':
                    conditions = [condition_creator(database_connection)]
                    while True:
                        multiple_conditions_choice = input('Would you like to include more conditions? '
                                                           'y for yes, anything else for no:    ')
                        # If user wishes to add more conditions, these will be appended to the list
                        if multiple_conditions_choice.lower() == 'y':
                            conditions.append(
                                condition_creator(database_connection))
                            continue
                        else:
                            break
                    order_choice = input(
                        'Would you like to order the result? y for yes and n for no:    ')
                    if order_choice.lower() == 'y':
                        order = order_by_creator(database_connection)
                        df = database_connection.query(
                            conditions, order, row_number)
                        print(df)
                    elif order_choice.lower() == 'n':
                        df = database_connection.query(
                            conditions, row_number=row_number)
                        # This prints the resulting DataFrame, but can be modified to e.g. save to a CSV file
                        print(df)
                    else:
                        print(
                            f'Sorry, {order_choice} was not one of the options, halting operation now...')
                elif conditions_choice.lower() == 'n':
                    order_choice = input(
                        'Would you like to order the result? y for yes and n for no:    ')
                    if order_choice.lower() == 'y':
                        order = order_by_creator(database_connection)
                        df = database_connection.query(
                            order=order, row_number=row_number)
                        print(df)
                    elif order_choice.lower() == 'n':
                        df = database_connection.query(row_number=row_number)
                        print(df)
                    else:
                        print(
                            f'Sorry, {order_choice} was not one of the options, halting operation now...')
                else:
                    print(
                        f'Sorry, {conditions_choice} was not one of the options, halting operation now...')
            elif number_choice.lower() == 'n':
                conditions_choice = input(
                    'Would you like to include conditions? y for yes and n for no:    ')
                if conditions_choice.lower() == 'y':
                    conditions = [condition_creator(database_connection)]
                    while True:
                        multiple_conditions_choice = input('Would you like to include more conditions? '
                                                           'y for yes, anything else for no:    ')
                        if multiple_conditions_choice.lower() == 'y':
                            conditions.append(
                                condition_creator(database_connection))
                            continue
                        else:
                            break
                    order_choice = input(
                        'Would you like to order the result? y for yes and n for no:    ')
                    if order_choice.lower() == 'y':
                        order = order_by_creator(database_connection)
                        df = database_connection.query(conditions, order)
                        print(df)
                    elif order_choice.lower() == 'n':
                        df = database_connection.query(conditions)
                        print(df)
                    else:
                        print(
                            f'Sorry, {order_choice} was not one of the options, halting operation now...')
                elif conditions_choice.lower() == 'n':
                    order_choice = input(
                        'Would you like to order the result? y for yes and n for no:    ')
                    if order_choice.lower() == 'y':
                        order = order_by_creator(database_connection)
                        df = database_connection.query(order=order)
                        print(df)
                    elif order_choice.lower() == 'n':
                        df = database_connection.query()
                        print(df)
                    else:
                        print(
                            f'Sorry, {order_choice} was not one of the options, halting operation now...')
                else:
                    print(
                        f'Sorry, {conditions_choice} was not one of the options, halting operation now...')
            else:
                print(
                    f'Sorry, {number_choice} was not one of the options, halting operation now...')
        elif user_choice == 's':
            filepath = input(
                'Please input the filepath you would like to save the file to:     ')
            try:
                database_connection.save_table(filepath)
            except FileNotFoundError:
                print('Sorry, that directory does not exist, halting operation now...')
        elif user_choice == 'u':
            print('To update rows, a CSV file is needed containing the rows to be added. This should also include '
                  'all columns present in the table')
            filepath = input('Please type the whole path of this data:    ')
            try:
                new_data = pd.read_csv(filepath)
                new_data = new_data.copy()
                columns = [column for column in new_data.columns]
                print('To know which rows to update, conditions must be given: \n')
                conditions = [condition_creator(database_connection)]
                while True:
                    multiple_conditions_choice = input('Would you like to include more conditions? '
                                                       'y for yes, anything else for no:    ')
                    if multiple_conditions_choice.lower() == 'y':
                        conditions.append(
                            condition_creator(database_connection))
                        continue
                    else:
                        break
                # Forces user to input conditions otherwise program won't know which rows to update
                database_connection.update_rows(columns, new_data, conditions)
            except FileNotFoundError:
                print('Sorry, that file does not exist, halting operation now...')
        else:
            print(
                f'Sorry, {user_choice} was not one of the options, halting operation now...')
        time.sleep(1)
        # Closes connection and cursor after processes have been completed
        database_connection.close_connection()
        # Offers the opportunity to run another process
        loop = input(
            'Would you like to do another operation? y for yes and anything else for no:      ')
        if loop.lower() == 'y':
            continue
        else:
            print('Closing program now...')
            time.sleep(1)
            break


if __name__ == '__main__':
    try:
        main()
    # Program will close when user inputs CTRL+C on keyboard
    except KeyboardInterrupt:
        print('User has halted program, closing now...')
