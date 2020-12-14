import os
import time
import pandas as pd
import datetime as dt
from tkinter import *
from tkinter import filedialog
import tkinter.font as font

from Database_Class import DatabaseConnection

root = Tk()
root.title('Python for Postgres')
root.geometry('450x450')

myFont = font.Font(font='Helvetica 14')

conditions = []


def add_columns(table, column_names_str, column_dtypes_str):
    new_columns = [
        item for item in column_names_str.replace(',', ' ').split()]
    new_dtypes = [
        item for item in column_dtypes_str.replace(',', ' ').split()]

    database_connection = DatabaseConnection(table)
    database_connection.add_columns(new_columns, new_dtypes)
    database_connection.close_connection()


def add_columns_selection(table, alter_frame):
    for widget in alter_frame.winfo_children():
        widget.destroy()

    add_columns_label = Label(
        alter_frame, text='Please present entries as comma separated list', font=myFont).grid(row=4, columnspan=2)

    add_columns_name_label = Label(
        alter_frame, text='New Column Names:', font=myFont).grid(row=5, column=0)
    add_columns_name_entry = Entry(
        alter_frame, width=15, font=myFont)
    add_columns_name_entry.grid(row=5, column=1)

    add_columns_dtype_label = Label(
        alter_frame, text='SQL Data Types of Columns:', font=myFont).grid(row=6, column=0)
    add_columns_dtype_entry = Entry(
        alter_frame, width=15, font=myFont)
    add_columns_dtype_entry.grid(row=6, column=1)

    submit_btn = Button(alter_frame, text='Submit', font=myFont, command=lambda: add_columns(
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

    rename_columns_label = Label(
        alter_frame, text='Please present entries as comma separated list', font=myFont).grid(row=4, columnspan=2)

    old_columns_name_label = Label(
        alter_frame, text='Columns To Be Renamed:', font=myFont).grid(row=5, column=0)
    old_columns_name_entry = Entry(
        alter_frame, width=15, font=myFont)
    old_columns_name_entry.grid(row=5, column=1)

    new_columns_name_label = Label(
        alter_frame, text='New Column Names:', font=myFont).grid(row=6, column=0)
    new_columns_name_entry = Entry(
        alter_frame, width=15, font=myFont)
    new_columns_name_entry.grid(row=6, column=1)

    submit_btn = Button(alter_frame, text='Submit', font=myFont, command=lambda: rename_columns(
        table, old_columns_name_entry.get(), new_columns_name_entry.get())).grid(row=7, columnspan=2, pady=10)


def rename_table(table, new_table_name):
    database_connection = DatabaseConnection(table)
    database_connection.rename_table(new_table_name)
    database_connection.close_connection()


def rename_table_selection(table, alter_frame):
    for widget in alter_frame.winfo_children():
        widget.destroy()

    rename_table_label = Label(
        alter_frame, text='What would you like to rename the table to?', font=myFont).grid(row=4, column=0)
    rename_table_entry = Entry(
        alter_frame, width=15, font=myFont)
    rename_table_entry.grid(row=5, column=0)

    submit_btn = Button(alter_frame, text='Submit', font=myFont, command=lambda: rename_table(
        table, rename_table_entry.get())).grid(row=6, column=0, pady=10)


def alter(table):
    for ele in root.winfo_children():
        ele.destroy()

    root.rowconfigure(0, weight=0)

    alter_frame = Frame(root)
    alter_frame.grid(row=4, columnspan=2, sticky="nesw")
    alter_frame.columnconfigure(0, weight=1)

    main_lbl = Label(root, text='How would you like to alter the table?',
                     font=myFont).grid(row=0, columnspan=2)

    option_1_btn = Button(root, text='Add', font=myFont, command=lambda: add_columns_selection(
        table, alter_frame)).grid(row=1, column=0, pady=5)
    option_1_lbl = Label(root, text='Add new columns',
                         font=myFont).grid(row=1, column=1, padx=10)

    option_2_btn = Button(root, text='Rename', font=myFont, command=lambda: rename_columns_selection(
        table, alter_frame)).grid(row=2, column=0, pady=5)
    option_2_lbl = Label(root, text='Rename columns',
                         font=myFont).grid(row=2, column=1, padx=10)

    option_3_btn = Button(root, text='Rename', font=myFont, command=lambda: rename_table_selection(
        table, alter_frame)).grid(row=3, column=0, pady=5)
    option_3_lbl = Label(root, text='Rename the table',
                         font=myFont).grid(row=3, column=1, padx=10)


def create_table(table, filepath, id_included):
    data = pd.read_csv(filepath)
    data = data.copy()
    columns = [column for column in data.columns]

    database_connection = DatabaseConnection(table)
    database_connection.create_table(columns, data, id_included)
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


def get_file(table, type):
    if type == 'create_new':
        id_included = BooleanVar()

        root.filename = filedialog.askopenfilename(
            initialdir=os.getcwd(), filetypes=[("csv files", "*.csv")])
        file_label = Label(root, text=f'Selected file: \n{root.filename}').grid(
            row=2, columnspan=2)

        checkbox = Checkbutton(root, text='Tick this if your data includes an index', font=myFont,
                               variable=id_included, onvalue=True, offvalue=False)
        checkbox.grid(row=3, columnspan=2)
        checkbox.deselect()
        submit_btn = Button(root, text='Submit', font=myFont, command=lambda: create_table(
            table, root.filename, id_included.get())).grid(row=4, columnspan=2, pady=10)
    elif type == 'insert_rows':
        root.filename = filedialog.askopenfilename(
            initialdir=os.getcwd(), filetypes=[("csv files", "*.csv")])
        file_label = Label(root, text=f'Selected file: \n{root.filename}').grid(
            row=2, columnspan=2)

        submit_btn = Button(root, text='Submit', font=myFont, command=lambda: insert_data(
            table, root.filename)).grid(row=3, columnspan=2, pady=10)
    elif type == 'save':
        root.filename = filedialog.asksaveasfile(
            mode='w', defaultextension='.csv', filetypes=[("csv files", "*.csv")])
        file_label = Label(root, text=f'Selected file: \n{root.filename.name}').grid(
            row=2, columnspan=2)

        submit_btn = Button(root, text='Save to File', font=myFont, command=lambda: save_data(
            table, root.filename.name)).grid(row=3, columnspan=2, pady=10)


def create(table):
    for ele in root.winfo_children():
        ele.destroy()

    main_lbl = Label(root, text='Please select a CSV file of the data \n for the program to create the table:',
                     font=myFont).grid(row=0, columnspan=2)

    select_btn = Button(root, text='Select', font=myFont, command=lambda: get_file(
        table, 'create_new')).grid(row=1, columnspan=2, pady=10)


def insert(table):
    for ele in root.winfo_children():
        ele.destroy()

    main_lbl = Label(root, text='Please select a CSV file of the data to insert:',
                     font=myFont).grid(row=0, columnspan=2)

    select_btn = Button(root, text='Select', font=myFont, command=lambda: get_file(
        table, 'insert_rows')).grid(row=1, columnspan=2, pady=10)


def save(table):
    for ele in root.winfo_children():
        ele.destroy()

    main_lbl = Label(root, text='Save your table as a CSV file:',
                     font=myFont).grid(row=0, columnspan=2)

    select_btn = Button(root, text='Select', font=myFont, command=lambda: get_file(
        table, 'save')).grid(row=1, columnspan=2, pady=10)


def drop_table(table):
    database_connection = DatabaseConnection(table)
    database_connection.drop_table()
    database_connection.close_connection()


def exit_program(frame):
    closing_lbl = Label(frame, text='Click to close program',
                        font=myFont).grid(row=6, columnspan=2)
    close_btn = Button(frame, text='Close', font=myFont,
                       command=root.quit).grid(row=7, columnspan=2, pady=10)


def drop_table_selection(table, delete_frame):
    for widget in delete_frame.winfo_children():
        widget.destroy()

    main_lbl = Label(
        delete_frame, text='Are you sure you would like to drop the table?', font=myFont).grid(row=4, columnspan=2)

    yes_btn = Button(delete_frame, text='Yes', font=myFont, command=lambda: drop_table(
        table)).grid(row=5, column=0, pady=10)
    no_btn = Button(delete_frame, text='No', font=myFont, command=lambda: exit_program(
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

    drop_columns_label = Label(
        delete_frame, text='Please present entries as comma separated list', font=myFont).grid(row=4, columnspan=2)

    drop_column_names_label = Label(
        delete_frame, text='Columns To Be Dropped:', font=myFont).grid(row=5, column=0)
    drop_column_names_entry = Entry(
        delete_frame, width=15, font=myFont)
    drop_column_names_entry.grid(row=5, column=1)

    submit_btn = Button(delete_frame, text='Submit', font=myFont, command=lambda: delete_columns(
        table, drop_column_names_entry.get())).grid(row=6, columnspan=2, pady=10)


def drop_rows(table, conditions):
    database_connection = DatabaseConnection(table)
    database_connection.delete_rows(conditions)
    database_connection.close_connection()


def condition_creator(table, frame):
    def equals(table, frame, column_name, values_str):
        global conditions
        values = [item for item in values_str.replace(',', ' ').split()]
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.equal(column_name, values))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        drop_rows_label = Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame)).grid(row=6, column=0, pady=10)
        no_btn = Button(frame, text='No', font=myFont, command=lambda: drop_rows(
            table, conditions)).grid(row=6, column=1, pady=10)

    def greater_than(table, frame, column_name, value):
        global conditions
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.greater_than(column_name, value))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        drop_rows_label = Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame)).grid(row=6, column=0, pady=10)
        no_btn = Button(frame, text='No', font=myFont, command=lambda: drop_rows(
            table, conditions)).grid(row=6, column=1, pady=10)

    def less_than(table, frame, column_name, value):
        global conditions
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.less_than(column_name, value))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        drop_rows_label = Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame)).grid(row=6, column=0, pady=10)
        no_btn = Button(frame, text='No', font=myFont, command=lambda: drop_rows(
            table, conditions)).grid(row=6, column=1, pady=10)

    def between(table, frame, column_name, start_value, end_value):
        global conditions
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.between(
            column_name, start_value, end_value))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        drop_rows_label = Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame)).grid(row=6, column=0, pady=10)
        no_btn = Button(frame, text='No', font=myFont, command=lambda: drop_rows(
            table, conditions)).grid(row=6, column=1, pady=10)

    def not_equal(table, frame, column_name, value):
        global conditions
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.not_equal(column_name, value))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        drop_rows_label = Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame)).grid(row=6, column=0, pady=10)
        no_btn = Button(frame, text='No', font=myFont, command=lambda: drop_rows(
            table, conditions)).grid(row=6, column=1, pady=10)

    def not_null(table, frame, column_name):
        global conditions
        database_connection = DatabaseConnection(table)
        conditions.append(database_connection.not_null(column_name))
        database_connection.close_connection()

        for widget in frame.winfo_children():
            widget.destroy()

        drop_rows_label = Label(
            frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
        yes_btn = Button(frame, text='Yes', font=myFont, command=lambda: condition_creator(
            table, frame)).grid(row=6, column=0, pady=10)
        no_btn = Button(frame, text='No', font=myFont, command=lambda: drop_rows(
            table, conditions)).grid(row=6, column=1, pady=10)

    def construct_condition(table, frame, column_name, comparator):
        submit_btn.destroy()

        if comparator == 'Equals':
            equal_value_label = Label(
                frame, text='What values would you like to compare?', font=myFont).grid(row=8, columnspan=2)
            equal_value_entry = Entry(frame, width=15, font=myFont)
            equal_value_entry.grid(row=9, columnspan=2)
            submit_condition_btn = Button(frame, text='Submit', font=myFont,
                                          command=lambda: equals(table, frame, column_name, equal_value_entry.get())).grid(row=10, columnspan=2, pady=10)

        elif comparator == 'Greater Than':
            greater_than_value_label = Label(
                frame, text='What value would you like to compare?', font=myFont).grid(row=8, columnspan=2)
            greater_than_value_entry = Entry(frame, width=15, font=myFont)
            greater_than_value_entry.grid(row=9, columnspan=2)
            submit_condition_btn = Button(frame, text='Submit', font=myFont,
                                          command=lambda: greater_than(table, frame, column_name, greater_than_value_entry.get())).grid(row=10, columnspan=2, pady=10)

        elif comparator == 'Less Than':
            less_than_value_label = Label(
                frame, text='What value would you like to compare?', font=myFont).grid(row=8, columnspan=2)
            less_than_value_entry = Entry(frame, width=15, font=myFont)
            less_than_value_entry.grid(row=9, columnspan=2)
            submit_condition_btn = Button(frame, text='Submit', font=myFont,
                                          command=lambda: less_than(table, frame, column_name, less_than_value_entry.get())).grid(row=10, columnspan=2, pady=10)

        elif comparator == 'Between':
            between_starting_value_label = Label(
                frame, text='What starting value would you like to use?', font=myFont).grid(row=8, columnspan=2)
            between_starting_value_entry = Entry(frame, width=15, font=myFont)
            between_starting_value_entry.grid(row=9, columnspan=2)
            between_ending_value_label = Label(
                frame, text='What ending value would you like to use?', font=myFont).grid(row=10, columnspan=2)
            between_ending_value_entry = Entry(frame, width=15, font=myFont)
            between_ending_value_entry.grid(row=11, columnspan=2)
            submit_condition_btn = Button(frame, text='Submit', font=myFont,
                                          command=lambda: between(table, frame, column_name, between_starting_value_entry.get(), between_ending_value_entry.get())).grid(row=12, columnspan=2, pady=10)

        elif comparator == 'Not Equal':
            not_equal_value_label = Label(
                frame, text='What value would you like to compare?', font=myFont).grid(row=8, columnspan=2)
            not_equal_value_entry = Entry(frame, width=15, font=myFont)
            not_equal_value_entry.grid(row=9, columnspan=2)
            submit_condition_btn = Button(frame, text='Submit', font=myFont,
                                          command=lambda: not_equal(table, frame, column_name, not_equal_value_entry.get())).grid(row=10, columnspan=2, pady=10)

        elif comparator == 'Not Null':
            not_null(table, frame, column_name)

    for widget in frame.winfo_children():
        widget.destroy()

    column_name_label = Label(
        frame, text='What column would you like to compare?', font=myFont).grid(row=4, columnspan=2)
    column_name_entry = Entry(frame, width=15, font=myFont)
    column_name_entry.grid(row=5, columnspan=2)

    comparator = StringVar()
    comparator.set('Equals')

    dropdown_label = Label(
        frame, text='What comparator would you like to use?', font=myFont).grid(row=6, columnspan=2)
    comparator_dropdown = OptionMenu(
        frame, comparator, 'Equals', 'Greater Than', 'Less Than', 'Between', 'Not Equal', 'Not Null')
    comparator_dropdown.grid(row=7, columnspan=2)

    submit_btn = Button(frame, text='Submit', font=myFont,
                        command=lambda: construct_condition(table, frame, column_name_entry.get(), comparator.get()))
    submit_btn.grid(row=8, columnspan=2, pady=10)


def drop_rows_selection(table, delete_frame):
    for widget in delete_frame.winfo_children():
        widget.destroy()

    drop_rows_warning = Label(
        delete_frame, text='Without conditions the program will drop all rows!', font=myFont).grid(row=4, columnspan=2)
    drop_rows_label = Label(
        delete_frame, text='Would you like to include more conditions?', font=myFont).grid(row=5, columnspan=2)
    yes_btn = Button(delete_frame, text='Yes', font=myFont, command=lambda: condition_creator(
        table, delete_frame)).grid(row=6, column=0, pady=10)
    no_btn = Button(delete_frame, text='No', font=myFont, command=lambda: drop_rows(
        table, None)).grid(row=6, column=1, pady=10)


def delete(table):
    for ele in root.winfo_children():
        ele.destroy()

    root.rowconfigure(0, weight=0)

    delete_frame = Frame(root)
    delete_frame.grid(row=4, columnspan=2, sticky="nesw")
    delete_frame.columnconfigure(0, weight=1)
    delete_frame.columnconfigure(1, weight=1)

    main_lbl = Label(root, text='What would you like to delete?',
                     font=myFont).grid(row=0, columnspan=2)

    option_1_btn = Button(root, text='Columns', font=myFont, command=lambda: delete_columns_selection(
        table, delete_frame)).grid(row=1, column=0, pady=5)
    option_1_lbl = Label(root, text='Delete columns',
                         font=myFont).grid(row=1, column=1, padx=10)

    option_2_btn = Button(root, text='Table', font=myFont, command=lambda: drop_table_selection(
        table, delete_frame)).grid(row=2, column=0, pady=5)
    option_2_lbl = Label(root, text='Drop whole table',
                         font=myFont).grid(row=2, column=1, padx=10)

    option_3_btn = Button(root, text='Rows', font=myFont, command=lambda: drop_rows_selection(
        table, delete_frame)).grid(row=3, column=0, pady=5)
    option_3_lbl = Label(root, text='Delete rows',
                         font=myFont).grid(row=3, column=1, padx=10)


def query(table):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to query').grid(row=0, column=0)


def update(table):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to update').grid(row=0, column=0)


def main_program(table):
    for ele in root.winfo_children():
        ele.destroy()

    frmMain = Frame(root)
    frmMain.grid(row=1, column=0, sticky="nesw")

    for i in range(2):
        frmMain.columnconfigure(i, weight=1)

    # database_connection = DatabaseConnection(table)

    main_lbl = Label(root, text='What would you like to do?', font=myFont).grid(
        row=0, column=0)

    alter_btn = Button(frmMain, text='Alter', font=myFont, command=lambda: alter(
        table)).grid(row=1, column=0, pady=5)
    alter_btn_lbl = Label(
        frmMain, text='Alter the existing table', font=myFont).grid(row=1, column=1)

    create_btn = Button(frmMain, text='Create', font=myFont, command=lambda: create(
        table)).grid(row=2, column=0, pady=5)
    create_btn_lbl = Label(
        frmMain, text='Create new table using this name', font=myFont).grid(row=2, column=1)

    delete_btn = Button(frmMain, text='Delete', font=myFont, command=lambda: delete(
        table)).grid(row=3, column=0, pady=5)
    delete_btn_lbl = Label(
        frmMain, text='Delete stuff in the table', font=myFont).grid(row=3, column=1)

    insert_btn = Button(frmMain, text='Insert', font=myFont, command=lambda: insert(
        table)).grid(row=4, column=0, pady=5)
    insert_btn_lbl = Label(
        frmMain, text='Insert new data into the table', font=myFont).grid(row=4, column=1)

    query_btn = Button(frmMain, text='Query', font=myFont, command=lambda: query(
        table)).grid(row=5, column=0, pady=5)
    query_btn_lbl = Label(
        frmMain, text='Query the data', font=myFont).grid(row=5, column=1)

    save_btn = Button(frmMain, text='Save', font=myFont, command=lambda: save(
        table)).grid(row=6, column=0, pady=5)
    save_btn_lbl = Label(
        frmMain, text='Save the table as a .csv', font=myFont).grid(row=6, column=1)

    update_btn = Button(frmMain, text='Update', font=myFont, command=lambda: update(
        table)).grid(row=7, column=0, pady=5)
    update_btn_lbl = Label(
        frmMain, text='Update the table\'s data', font=myFont).grid(row=7, column=1)


root.columnconfigure(0, weight=1)

frame = Frame(root)
frame.grid(row=1, column=0, sticky='nsew')

for num in range(2):
    frame.columnconfigure(num, weight=1)

current_time = str(dt.datetime.now().strftime('%d/%m/%Y %H:%M'))
intro_lbl_text = '\n' + current_time + '\n'
intro_lbl_text += 75 * '=' + '\n'
intro_lbl_text += 'Welcome to Python for Postgres! \n'

intro_lbl = Label(root, text=intro_lbl_text, font=myFont).grid(
    row=0, column=0)

table_entry_lbl = Label(
    frame, text='SQL table name:', font=myFont)
table_entry_lbl.grid(row=1, column=0)
table_entry = Entry(frame, width=15, font=myFont)
table_entry.grid(row=1, column=1)
table_entry_submit = Button(
    frame, text='Submit', font=myFont, command=lambda: main_program(table_entry.get()))
table_entry_submit.grid(row=2, columnspan=2, pady=10,
                        ipadx=5, ipady=5)

root.mainloop()
