import os
import pandas as pd
import datetime as dt
import tkinter as tk
from tkinter import filedialog, ttk
import tkinter.font as font

from Database_Class import DatabaseConnection


def add_columns(table, column_names_str, column_dtypes_str):
    # Converts the string variable to a list with each column name an element
    new_columns = [
        item for item in column_names_str.replace(',', ' ').split()]
    new_dtypes = [
        item for item in column_dtypes_str.replace(',', ' ').split()]

    database_connection = DatabaseConnection(table)
    database_connection.add_columns(new_columns, new_dtypes)
    database_connection.close_connection()


def add_columns_selection(table, alter_frame):
    # Clears the frame
    for widget in alter_frame.winfo_children():
        widget.destroy()

    add_columns_label = tk.Label(
        alter_frame, text='Please present entries as comma separated list', font=myFont).grid(row=4, columnspan=2)

    add_columns_name_label = tk.Label(
        alter_frame, text='New Column Names:', font=myFont).grid(row=5, column=0)
    add_columns_name_entry = tk.Entry(
        alter_frame, width=15, font=myFont)
    add_columns_name_entry.grid(row=5, column=1)

    add_columns_dtype_label = tk.Label(
        alter_frame, text='SQL Data Types of Columns:', font=myFont).grid(row=6, column=0)
    add_columns_dtype_entry = tk.Entry(
        alter_frame, width=15, font=myFont)
    add_columns_dtype_entry.grid(row=6, column=1)

    submit_btn = tk.Button(alter_frame, text='Submit', font=myFont, command=lambda: add_columns(
        table, add_columns_name_entry.get(), add_columns_dtype_entry.get())).grid(row=7, columnspan=2, pady=10)


def rename_columns(table, old_column_names_str, new_column_names_str):
    old_column_names = [
        item for item in old_column_names_str.replace(',', ' ').split()]
    new_column_names = [
        item for item in new_column_names_str.replace(',', ' ').split()]

    database_connection = DatabaseConnection(table)
    database_connection.rename_columns(old_column_names, new_column_names)
    database_connection.close_connection()


def rename_columns_selection(table, alter_frame):
    for widget in alter_frame.winfo_children():
        widget.destroy()

    rename_columns_label = tk.Label(
        alter_frame, text='Please present entries as comma separated list', font=myFont).grid(row=4, columnspan=2)

    old_columns_name_label = tk.Label(
        alter_frame, text='Columns To Be Renamed:', font=myFont).grid(row=5, column=0)
    old_columns_name_entry = tk.Entry(
        alter_frame, width=15, font=myFont)
    old_columns_name_entry.grid(row=5, column=1)

    new_columns_name_label = tk.Label(
        alter_frame, text='New Column Names:', font=myFont).grid(row=6, column=0)
    new_columns_name_entry = tk.Entry(
        alter_frame, width=15, font=myFont)
    new_columns_name_entry.grid(row=6, column=1)

    submit_btn = tk.Button(alter_frame, text='Submit', font=myFont, command=lambda: rename_columns(
        table, old_columns_name_entry.get(), new_columns_name_entry.get())).grid(row=7, columnspan=2, pady=10)


def rename_table(table, new_table_name):
    database_connection = DatabaseConnection(table)
    database_connection.rename_table(new_table_name)
    database_connection.close_connection()


def rename_table_selection(table, alter_frame):
    for widget in alter_frame.winfo_children():
        widget.destroy()

    rename_table_label = tk.Label(
        alter_frame, text='What would you like to rename the table to?', font=myFont).grid(row=4, column=0)
    rename_table_entry = tk.Entry(
        alter_frame, width=15, font=myFont)
    rename_table_entry.grid(row=5, column=0)

    submit_btn = tk.Button(alter_frame, text='Submit', font=myFont, command=lambda: rename_table(
        table, rename_table_entry.get())).grid(row=6, column=0, pady=10)


def alter(table):
    # Clears the root window
    for ele in root.winfo_children():
        ele.destroy()

    root.rowconfigure(0, weight=0)

    alter_frame = tk.Frame(root)
    alter_frame.grid(row=4, columnspan=2, sticky="nesw")
    alter_frame.columnconfigure(0, weight=1)

    main_lbl = tk.Label(root, text='How would you like to alter the table?',
                        font=myFont).grid(row=0, columnspan=2)

    option_1_btn = tk.Button(root, text='Add', font=myFont, command=lambda: add_columns_selection(
        table, alter_frame)).grid(row=1, column=0, pady=5)
    option_1_lbl = tk.Label(root, text='Add new columns',
                            font=myFont).grid(row=1, column=1, padx=10)

    option_2_btn = tk.Button(root, text='Rename', font=myFont, command=lambda: rename_columns_selection(
        table, alter_frame)).grid(row=2, column=0, pady=5)
    option_2_lbl = tk.Label(root, text='Rename columns',
                            font=myFont).grid(row=2, column=1, padx=10)

    option_3_btn = tk.Button(root, text='Rename', font=myFont, command=lambda: rename_table_selection(
        table, alter_frame)).grid(row=3, column=0, pady=5)
    option_3_lbl = tk.Label(root, text='Rename the table',
                            font=myFont).grid(row=3, column=1, padx=10)


def create_table(table, filepath, id_included, create_and_insert):
    # Reads in DataFrame using pandas
    data = pd.read_csv(filepath)
    data = data.copy()
    # Stores each column name in a list
    columns = [column for column in data.columns]

    database_connection = DatabaseConnection(table)
    database_connection.create_table(columns, data, id_included)

    # If the user wishes to insert data as well, the program is able to carry both these out
    if (create_and_insert):
        database_connection.insert_rows(columns, data)

    database_connection.close_connection()


def insert_data(table, filepath):
    data = pd.read_csv(filepath)
    data = data.copy()
    columns = [column for column in data.columns]

    database_connection = DatabaseConnection(table)
    database_connection.insert_rows(columns, data)
    database_connection.close_connection()


def save_data(table, filepath):
    database_connection = DatabaseConnection(table)
    database_connection.save_table(filepath)
    database_connection.close_connection()


def drop_rows(table, conditions):
    database_connection = DatabaseConnection(table)
    database_connection.delete_rows(conditions)
    database_connection.close_connection()


def update_rows(table, conditions, filepath):
    print(filepath)
    new_data = pd.read_csv(filepath)
    new_data = new_data.copy()
    columns = [column for column in new_data.columns]

    database_connection = DatabaseConnection(table)
    database_connection.update_rows(columns, new_data, conditions)
    database_connection.close_connection()


def query_data(table, frame, conditions=None, limit=None, order=None):
    # Converts the limit to an integer if it has been specified
    if limit is not None:
        limit_int = int(limit)
    else:
        limit_int = None

    database_connection = DatabaseConnection(table)
    # Obtains query result as a DataFrame
    df = database_connection.query(conditions, order, limit_int)
    database_connection.close_connection()

    for widget in root.winfo_children():
        widget.destroy()

    root.pack_propagate(False)
    root.resizable(0, 0)

    data_frame = tk.LabelFrame(root, text="Results", font=myFont)
    data_frame.place(height=450, width=450)

    data_treeview = ttk.Treeview(data_frame)
    data_treeview.place(relheight=1, relwidth=1)

    treescrolly = tk.Scrollbar(data_frame, orient="vertical",
                               command=data_treeview.yview)
    treescrollx = tk.Scrollbar(
        data_frame, orient="horizontal", command=data_treeview.xview)
    data_treeview.configure(xscrollcommand=treescrollx.set,
                            yscrollcommand=treescrolly.set)
    treescrollx.pack(side="bottom", fill="x")
    treescrolly.pack(side="right", fill="y")

    data_treeview["column"] = list(df.columns)
    data_treeview["show"] = "headings"

    for column in data_treeview["columns"]:
        # let the column heading = column name
        data_treeview.heading(column, text=column)

    # turns the dataframe into a list of lists
    df_rows = df.to_numpy().tolist()

    # Displays the data in a TreeView: similar to a table
    for row in df_rows:
        data_treeview.insert("", "end", values=row)


def create_order_by_statement(table, frame, column_name, direction, conditions=None, limit=None):
    database_connection = DatabaseConnection(table)

    if direction == 'Ascending':
        order = database_connection.asc(column_name)
    elif direction == 'Descending':
        order = database_connection.desc(column_name)

    database_connection.close_connection()

    query_data(table, frame, conditions, limit, order)


def construct_order_by(table, frame, conditions=None, limit=None):
    for widget in frame.winfo_children():
        widget.destroy()

    order_query_label = tk.Label(
        frame, text='What column would you like to order the data by?', font=myFont).grid(row=1, columnspan=2)
    order_entry = tk.Entry(frame, width=15, font=myFont)
    order_entry.grid(row=2, columnspan=2)

    # Initialises variable to be used in Radiobuttons
    order_direction = tk.StringVar()
    order_direction.set('Ascending')

    order_asc_radio_btn = tk.Radiobutton(
        frame, text='Ascending', variable=order_direction, value='Ascending', font=myFont).grid(row=3, column=0)
    order_desc_radio_btn = tk.Radiobutton(
        frame, text='Descending', variable=order_direction, value='Descending', font=myFont).grid(row=3, column=1)

    submit_btn = tk.Button(frame, text='Submit', font=myFont, command=lambda: create_order_by_statement(
        table, frame, order_entry.get(), order_direction.get(), conditions, limit)).grid(row=4, columnspan=2)


def order_by_creator(table, frame, conditions=None, limit=None):
    for widget in frame.winfo_children():
        widget.destroy()

    limit_result_label = tk.Label(
        frame, text='Would you like to order your results?', font=myFont).grid(row=0, columnspan=2)
    yes_btn = tk.Button(frame, text='Yes', font=myFont, command=lambda: construct_order_by(
        table, frame, conditions, limit)).grid(row=1, column=0, pady=10)
    no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: query_data(
        table, frame, conditions, limit)).grid(row=1, column=1, pady=10)


def construct_limit(table, frame, conditions=None):
    for widget in frame.winfo_children():
        widget.destroy()

    limit_warning_label = tk.Label(
        frame, text='Please only input an integer value', font=myFont).grid(row=0, columnspan=2)
    limit_query_label = tk.Label(
        frame, text='How many results would you like returned?', font=myFont).grid(row=1, columnspan=2)
    limit_entry = tk.Entry(frame, width=15, font=myFont)
    limit_entry.grid(row=2, columnspan=2)
    submit_btn = tk.Button(frame, text='Submit', font=myFont, command=lambda: order_by_creator(
        table, frame, conditions, limit_entry.get())).grid(row=3, columnspan=2)


def limit_creator(table, frame, conditions):
    for widget in frame.winfo_children():
        widget.destroy()

    limit_result_label = tk.Label(
        frame, text='Would you like to limit your results?', font=myFont).grid(row=0, columnspan=2)
    yes_btn = tk.Button(frame, text='Yes', font=myFont, command=lambda: construct_limit(
        table, frame, conditions)).grid(row=1, column=0, pady=10)
    no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: order_by_creator(
        table, frame, conditions)).grid(row=1, column=1, pady=10)


def condition_creator(table, frame, type, filepath=None):
    def equals(table, frame, column_name, values_str, type, filepath=None):
        # Allows for any changes to conditions in this function to be registered globally
        global conditions
        # Allows for multiple values to be inputted for equivalence
        values = [item for item in values_str.replace(',', ' ').split()]
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.equal(column_name, values))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        main_label = tk.Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = tk.Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame, type, filepath)).grid(row=6, column=0, pady=10)
        if type == 'drop_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: drop_rows(
                table, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'query':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: limit_creator(
                table, frame, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'update_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: update_rows(
                table, conditions, filepath)).grid(row=6, column=1, pady=10)

    def greater_than(table, frame, column_name, value, type, filepath=None):
        global conditions
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.greater_than(column_name, value))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        main_label = tk.Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = tk.Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame, type, filepath)).grid(row=6, column=0, pady=10)
        if type == 'drop_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: drop_rows(
                table, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'query':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: limit_creator(
                table, frame, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'update_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: update_rows(
                table, conditions, filepath)).grid(row=6, column=1, pady=10)

    def less_than(table, frame, column_name, value, type, filepath=None):
        global conditions
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.less_than(column_name, value))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        main_label = tk.Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = tk.Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame, type, filepath)).grid(row=6, column=0, pady=10)
        if type == 'drop_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: drop_rows(
                table, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'query':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: limit_creator(
                table, frame, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'update_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: update_rows(
                table, conditions, filepath)).grid(row=6, column=1, pady=10)

    def between(table, frame, column_name, start_value, end_value, type, filepath=None):
        global conditions
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.between(
            column_name, start_value, end_value))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        main_label = tk.Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = tk.Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame, type, filepath)).grid(row=6, column=0, pady=10)
        if type == 'drop_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: drop_rows(
                table, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'query':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: limit_creator(
                table, frame, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'update_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: update_rows(
                table, conditions, filepath)).grid(row=6, column=1, pady=10)

    def not_equal(table, frame, column_name, value, type, filepath=None):
        global conditions
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.not_equal(column_name, value))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        main_label = tk.Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = tk.Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame, type, filepath)).grid(row=6, column=0, pady=10)
        if type == 'drop_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: drop_rows(
                table, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'query':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: limit_creator(
                table, frame, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'update_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: update_rows(
                table, conditions, filepath)).grid(row=6, column=1, pady=10)

    def not_null(table, frame, column_name, type, filepath=None):
        global conditions
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.not_null(column_name))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        main_label = tk.Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = tk.Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame, type, filepath)).grid(row=6, column=0, pady=10)
        if type == 'drop_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: drop_rows(
                table, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'query':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: limit_creator(
                table, frame, conditions)).grid(row=6, column=1, pady=10)
        elif type == 'update_rows':
            no_btn = tk.Button(frame, text='No', font=myFont, command=lambda: update_rows(
                table, conditions, filepath)).grid(row=6, column=1, pady=10)

    def construct_condition(table, frame, column_name, comparator, type, filepath=None):
        submit_btn.destroy()

        if comparator == 'Equals':
            equal_value_label = tk.Label(
                frame, text='What values would you like to compare?', font=myFont).grid(row=9, columnspan=2)
            equal_value_entry = tk.Entry(frame, width=15, font=myFont)
            equal_value_entry.grid(row=10, columnspan=2)
            submit_condition_btn = tk.Button(frame, text='Submit', font=myFont,
                                             command=lambda: equals(table, frame, column_name, equal_value_entry.get(), type, filepath)).grid(row=11, columnspan=2, pady=10)

        elif comparator == 'Greater Than':
            greater_than_value_label = tk.Label(
                frame, text='What value would you like to compare?', font=myFont).grid(row=9, columnspan=2)
            greater_than_value_entry = tk.Entry(frame, width=15, font=myFont)
            greater_than_value_entry.grid(row=10, columnspan=2)
            submit_condition_btn = tk.Button(frame, text='Submit', font=myFont,
                                             command=lambda: greater_than(table, frame, column_name, greater_than_value_entry.get(), type, filepath)).grid(row=11, columnspan=2, pady=10)

        elif comparator == 'Less Than':
            less_than_value_label = tk.Label(
                frame, text='What value would you like to compare?', font=myFont).grid(row=9, columnspan=2)
            less_than_value_entry = tk.Entry(frame, width=15, font=myFont)
            less_than_value_entry.grid(row=10, columnspan=2)
            submit_condition_btn = tk.Button(frame, text='Submit', font=myFont,
                                             command=lambda: less_than(table, frame, column_name, less_than_value_entry.get(), type, filepath)).grid(row=11, columnspan=2, pady=10)

        elif comparator == 'Between':
            between_starting_value_label = tk.Label(
                frame, text='What starting value would you like to use?', font=myFont).grid(row=9, columnspan=2)
            between_starting_value_entry = tk.Entry(
                frame, width=15, font=myFont)
            between_starting_value_entry.grid(row=10, columnspan=2)
            between_ending_value_label = tk.Label(
                frame, text='What ending value would you like to use?', font=myFont).grid(row=11, columnspan=2)
            between_ending_value_entry = tk.Entry(frame, width=15, font=myFont)
            between_ending_value_entry.grid(row=12, columnspan=2)
            submit_condition_btn = tk.Button(frame, text='Submit', font=myFont,
                                             command=lambda: between(table, frame, column_name, between_starting_value_entry.get(), between_ending_value_entry.get(), type, filepath)).grid(row=13, columnspan=2, pady=10)

        elif comparator == 'Not Equal':
            not_equal_value_label = tk.Label(
                frame, text='What value would you like to compare?', font=myFont).grid(row=9, columnspan=2)
            not_equal_value_entry = tk.Entry(frame, width=15, font=myFont)
            not_equal_value_entry.grid(row=10, columnspan=2)
            submit_condition_btn = tk.Button(frame, text='Submit', font=myFont,
                                             command=lambda: not_equal(table, frame, column_name, not_equal_value_entry.get(), type, filepath)).grid(row=11, columnspan=2, pady=10)

        elif comparator == 'Not Null':
            not_null(table, frame, column_name, type, filepath)

    if type == 'drop_rows' or type == 'query':
        for widget in frame.winfo_children():
            widget.destroy()

        column_name_label = tk.Label(
            frame, text='What column would you like to compare?', font=myFont).grid(row=4, columnspan=2)
        column_name_entry = tk.Entry(frame, width=15, font=myFont)
        column_name_entry.grid(row=5, columnspan=2)

        comparator = tk.StringVar()
        comparator.set('Equals')

        dropdown_label = tk.Label(
            frame, text='What comparator would you like to use?', font=myFont).grid(row=6, columnspan=2)
        comparator_dropdown = tk.OptionMenu(
            frame, comparator, 'Equals', 'Greater Than', 'Less Than', 'Between', 'Not Equal', 'Not Null')
        comparator_dropdown.grid(row=7, columnspan=2)

        submit_btn = tk.Button(frame, text='Submit', font=myFont,
                               command=lambda: construct_condition(table, frame, column_name_entry.get(), comparator.get(), type))
        submit_btn.grid(row=8, columnspan=2, pady=10)

    elif type == 'update_rows':
        # Will pass the filepath to construct_condition if updating rows
        for widget in frame.winfo_children():
            widget.destroy()

        condition_label = tk.Label(
            frame, text='To update you must input conditions:', font=myFont).grid(row=4, columnspan=2)
        column_name_label = tk.Label(
            frame, text='What column would you like to compare?', font=myFont).grid(row=5, columnspan=2)
        column_name_entry = tk.Entry(frame, width=15, font=myFont)
        column_name_entry.grid(row=6, columnspan=2)

        comparator = tk.StringVar()
        comparator.set('Equals')

        dropdown_label = tk.Label(
            frame, text='What comparator would you like to use?', font=myFont).grid(row=7, columnspan=2)
        comparator_dropdown = tk.OptionMenu(
            frame, comparator, 'Equals', 'Greater Than', 'Less Than', 'Between', 'Not Equal', 'Not Null')
        comparator_dropdown.grid(row=8, columnspan=2)

        submit_btn = tk.Button(frame, text='Submit', font=myFont,
                               command=lambda: construct_condition(table, frame, column_name_entry.get(), comparator.get(), type, filepath))
        submit_btn.grid(row=9, columnspan=2, pady=10)


def get_file(table, type, frame=None):
    if type == 'create_new':
        # Will ask the user if there is an index or if they would like to concurrently insert new data when creating new table
        id_included = tk.BooleanVar()
        create_and_insert = tk.BooleanVar()

        root.filename = filedialog.askopenfilename(
            initialdir=os.getcwd(), filetypes=[("csv files", "*.csv")])
        file_label = tk.Label(root, text=f'Selected file: \n{root.filename}').grid(
            row=2, columnspan=2)

        checkbox_index = tk.Checkbutton(root, text='Tick this if your data includes an index', font=myFont,
                                        variable=id_included, onvalue=True, offvalue=False)
        checkbox_index.grid(row=3, columnspan=2)
        checkbox_index.deselect()

        checkbox_insert = tk.Checkbutton(root, text='Tick this if you would also like to insert data', font=myFont,
                                         variable=create_and_insert, onvalue=True, offvalue=False)
        checkbox_insert.grid(row=4, columnspan=2)
        checkbox_insert.deselect()

        submit_btn = tk.Button(root, text='Submit', font=myFont, command=lambda: create_table(
            table, root.filename, id_included.get(), create_and_insert.get())).grid(row=5, columnspan=2, pady=10)
    elif type == 'insert_rows':
        root.filename = filedialog.askopenfilename(
            initialdir=os.getcwd(), filetypes=[("csv files", "*.csv")])
        file_label = tk.Label(root, text=f'Selected file: \n{root.filename}').grid(
            row=2, columnspan=2)

        submit_btn = tk.Button(root, text='Submit', font=myFont, command=lambda: insert_data(
            table, root.filename)).grid(row=3, columnspan=2, pady=10)
    elif type == 'update_rows':
        root.filename = filedialog.askopenfilename(
            initialdir=os.getcwd(), filetypes=[("csv files", "*.csv")])
        file_label = tk.Label(frame, text=f'Selected file: \n{root.filename}').grid(
            row=2, columnspan=2)

        submit_btn = tk.Button(frame, text='Submit', font=myFont, command=lambda: condition_creator(
            table, frame, 'update_rows', root.filename)).grid(row=3, columnspan=2, pady=10)
    elif type == 'save':
        # If saving data, the program will ask the user to save to a new CSV file and it will produce this file for them
        # after this, it gives the option to write to the file, which will save the table to the CSV using pandas
        root.filename = filedialog.asksaveasfile(
            mode='w', defaultextension='.csv', filetypes=[("csv files", "*.csv")])
        file_label = tk.Label(root, text=f'Selected file: \n{root.filename.name}').grid(
            row=2, columnspan=2)

        submit_btn = tk.Button(root, text='Write to File', font=myFont, command=lambda: save_data(
            table, root.filename.name)).grid(row=3, columnspan=2, pady=10)


def create(table):
    for ele in root.winfo_children():
        ele.destroy()

    main_lbl = tk.Label(root, text='Please select a CSV file of the data \n for the program to create the table:',
                        font=myFont).grid(row=0, columnspan=2)

    select_btn = tk.Button(root, text='Select', font=myFont, command=lambda: get_file(
        table, 'create_new')).grid(row=1, columnspan=2, pady=10)


def insert(table):
    for ele in root.winfo_children():
        ele.destroy()

    main_lbl = tk.Label(root, text='Please select a CSV file of the data to insert:',
                        font=myFont).grid(row=0, columnspan=2)

    select_btn = tk.Button(root, text='Select', font=myFont, command=lambda: get_file(
        table, 'insert_rows')).grid(row=1, columnspan=2, pady=10)


def save(table):
    for ele in root.winfo_children():
        ele.destroy()

    main_lbl = tk.Label(root, text='Save your table as a CSV file:',
                        font=myFont).grid(row=0, columnspan=2)

    select_btn = tk.Button(root, text='Select', font=myFont, command=lambda: get_file(
        table, 'save')).grid(row=1, columnspan=2, pady=10)


def drop_table(table):
    database_connection = DatabaseConnection(table)
    database_connection.drop_table()
    database_connection.close_connection()


def exit_program(frame):
    # Closes the program for the user if they back out of dropping the table
    closing_lbl = tk.Label(frame, text='Click to close program',
                           font=myFont).grid(row=6, columnspan=2)
    close_btn = tk.Button(frame, text='Close', font=myFont,
                          command=root.quit).grid(row=7, columnspan=2, pady=10)


def drop_table_selection(table, delete_frame):
    for widget in delete_frame.winfo_children():
        widget.destroy()

    main_lbl = tk.Label(
        delete_frame, text='Are you sure you would like to drop the table?', font=myFont).grid(row=4, columnspan=2)

    yes_btn = tk.Button(delete_frame, text='Yes', font=myFont, command=lambda: drop_table(
        table)).grid(row=5, column=0, pady=10)
    no_btn = tk.Button(delete_frame, text='No', font=myFont, command=lambda: exit_program(
        delete_frame)).grid(row=5, column=1, pady=10)


def delete_columns(table, drop_columns_str):
    drop_columns = [
        item for item in drop_columns_str.replace(',', ' ').split()]

    database_connection = DatabaseConnection(table)
    database_connection.drop_columns(drop_columns)
    database_connection.close_connection()


def delete_columns_selection(table, delete_frame):
    for widget in delete_frame.winfo_children():
        widget.destroy()

    drop_columns_label = tk.Label(
        delete_frame, text='Please present entries as comma separated list', font=myFont).grid(row=4, columnspan=2)

    drop_column_names_label = tk.Label(
        delete_frame, text='Columns To Be Dropped:', font=myFont).grid(row=5, column=0)
    drop_column_names_entry = tk.Entry(
        delete_frame, width=15, font=myFont)
    drop_column_names_entry.grid(row=5, column=1)

    submit_btn = tk.Button(delete_frame, text='Submit', font=myFont, command=lambda: delete_columns(
        table, drop_column_names_entry.get())).grid(row=6, columnspan=2, pady=10)


def drop_rows_selection(table, delete_frame):
    for widget in delete_frame.winfo_children():
        widget.destroy()

    drop_rows_warning = tk.Label(
        delete_frame, text='Without conditions the program will drop all rows!', font=myFont).grid(row=4, columnspan=2)
    drop_rows_label = tk.Label(
        delete_frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
    yes_btn = tk.Button(delete_frame, text='Yes', font=myFont, command=lambda: condition_creator(
        table, delete_frame, 'drop_rows')).grid(row=6, column=0, pady=10)
    no_btn = tk.Button(delete_frame, text='No', font=myFont, command=lambda: drop_rows(
        table, None)).grid(row=6, column=1, pady=10)


def delete(table):
    for ele in root.winfo_children():
        ele.destroy()

    root.rowconfigure(0, weight=0)

    delete_frame = tk.Frame(root)
    delete_frame.grid(row=4, columnspan=2, sticky="nesw")
    delete_frame.columnconfigure(0, weight=1)
    delete_frame.columnconfigure(1, weight=1)

    main_lbl = tk.Label(root, text='What would you like to delete?',
                        font=myFont).grid(row=0, columnspan=2)

    option_1_btn = tk.Button(root, text='Columns', font=myFont, command=lambda: delete_columns_selection(
        table, delete_frame)).grid(row=1, column=0, pady=5)
    option_1_lbl = tk.Label(root, text='Delete columns',
                            font=myFont).grid(row=1, column=1, padx=10)

    option_2_btn = tk.Button(root, text='Table', font=myFont, command=lambda: drop_table_selection(
        table, delete_frame)).grid(row=2, column=0, pady=5)
    option_2_lbl = tk.Label(root, text='Drop whole table',
                            font=myFont).grid(row=2, column=1, padx=10)

    option_3_btn = tk.Button(root, text='Rows', font=myFont, command=lambda: drop_rows_selection(
        table, delete_frame)).grid(row=3, column=0, pady=5)
    option_3_lbl = tk.Label(root, text='Delete rows',
                            font=myFont).grid(row=3, column=1, padx=10)


def query(table):
    for ele in root.winfo_children():
        ele.destroy()

    root.rowconfigure(0, weight=0)

    query_frame = tk.Frame(root)
    query_frame.grid(row=4, columnspan=2, sticky="nesw")
    query_frame.columnconfigure(0, weight=1)
    query_frame.columnconfigure(1, weight=1)

    query_warning = tk.Label(
        query_frame, text='Without conditions the program will return all data', font=myFont).grid(row=0, columnspan=2)
    query_label = tk.Label(
        query_frame, text='Would you like to include more conditions?', font=myFont).grid(row=1, columnspan=2)
    yes_btn = tk.Button(query_frame, text='Yes', font=myFont, command=lambda: condition_creator(
        table, query_frame, 'query')).grid(row=2, column=0, pady=10)
    no_btn = tk.Button(query_frame, text='No', font=myFont, command=lambda: limit_creator(
        table, query_frame, None)).grid(row=2, column=1, pady=10)


def update(table):
    for ele in root.winfo_children():
        ele.destroy()

    root.rowconfigure(0, weight=0)

    update_frame = tk.Frame(root)
    update_frame.grid(row=4, columnspan=2, sticky="nesw")
    update_frame.columnconfigure(0, weight=1)
    update_frame.columnconfigure(1, weight=1)

    main_lbl = tk.Label(update_frame, text='Please select a CSV file containing rows to update:',
                        font=myFont).grid(row=0, columnspan=2)

    select_btn = tk.Button(update_frame, text='Select', font=myFont, command=lambda: get_file(
        table, 'update_rows', update_frame)).grid(row=1, columnspan=2, pady=10)


def main_program(table):
    # Lays out all options for the user to carry out in this program
    for ele in root.winfo_children():
        ele.destroy()

    frmMain = tk.Frame(root)
    frmMain.grid(row=1, column=0, sticky="nesw")

    for i in range(2):
        frmMain.columnconfigure(i, weight=1)

    main_lbl = tk.Label(root, text='What would you like to do?', font=myFont).grid(
        row=0, column=0)

    alter_btn = tk.Button(frmMain, text='Alter', font=myFont, command=lambda: alter(
        table)).grid(row=1, column=0, pady=5)
    alter_btn_lbl = tk.Label(
        frmMain, text='Alter the existing table', font=myFont).grid(row=1, column=1)

    create_btn = tk.Button(frmMain, text='Create', font=myFont, command=lambda: create(
        table)).grid(row=2, column=0, pady=5)
    create_btn_lbl = tk.Label(
        frmMain, text='Create new table using this name', font=myFont).grid(row=2, column=1)

    delete_btn = tk.Button(frmMain, text='Delete', font=myFont, command=lambda: delete(
        table)).grid(row=3, column=0, pady=5)
    delete_btn_lbl = tk.Label(
        frmMain, text='Delete stuff in the table', font=myFont).grid(row=3, column=1)

    insert_btn = tk.Button(frmMain, text='Insert', font=myFont, command=lambda: insert(
        table)).grid(row=4, column=0, pady=5)
    insert_btn_lbl = tk.Label(
        frmMain, text='Insert new data into the table', font=myFont).grid(row=4, column=1)

    query_btn = tk.Button(frmMain, text='Query', font=myFont, command=lambda: query(
        table)).grid(row=5, column=0, pady=5)
    query_btn_lbl = tk.Label(
        frmMain, text='Query the data', font=myFont).grid(row=5, column=1)

    save_btn = tk.Button(frmMain, text='Save', font=myFont, command=lambda: save(
        table)).grid(row=6, column=0, pady=5)
    save_btn_lbl = tk.Label(
        frmMain, text='Save the table as a .csv file', font=myFont).grid(row=6, column=1)

    update_btn = tk.Button(frmMain, text='Update', font=myFont, command=lambda: update(
        table)).grid(row=7, column=0, pady=5)
    update_btn_lbl = tk.Label(
        frmMain, text='Update the table\'s data', font=myFont).grid(row=7, column=1)


if __name__ == '__main__':
    # Creates instance of main window
    root = tk.Tk()
    root.title('Python for Postgres')
    # Sizes the window
    root.geometry('450x450')

    myFont = font.Font(font='Helvetica 14')

    conditions = []

    root.columnconfigure(0, weight=1)

    frame = tk.Frame(root)
    frame.grid(row=1, column=0, sticky='nsew')

    for num in range(2):
        frame.columnconfigure(num, weight=1)

    current_time = str(dt.datetime.now().strftime('%d/%m/%Y %H:%M'))
    intro_lbl_text = '\n' + current_time + '\n'
    intro_lbl_text += 75 * '=' + '\n'
    intro_lbl_text += 'Welcome to Python for Postgres! \n'

    intro_lbl = tk.Label(root, text=intro_lbl_text, font=myFont).grid(
        row=0, column=0)

    table_entry_lbl = tk.Label(
        frame, text='SQL table name:', font=myFont)
    table_entry_lbl.grid(row=1, column=0)
    table_entry = tk.Entry(frame, width=15, font=myFont)
    table_entry.grid(row=1, column=1)
    table_entry_submit = tk.Button(
        frame, text='Submit', font=myFont, command=lambda: main_program(table_entry.get()))
    table_entry_submit.grid(row=2, columnspan=2, pady=10,
                            ipadx=5, ipady=5)

    # Runs the window
    root.mainloop()
