import time
import pandas as pd
import datetime as dt
from tkinter import *
import tkinter.font as font

from Database_Class import DatabaseConnection

root = Tk()
root.title('Python for Postgres')
root.geometry('450x450')

myFont = font.Font(font='Helvetica 14')


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
        alter_frame, text='What would you like to rename the table to?', font=myFont).grid(row=4, columnspan=2)
    rename_table_entry = Entry(
        alter_frame, width=15, font=myFont)
    rename_table_entry.grid(row=5, columnspan=2)

    submit_btn = Button(alter_frame, text='Submit', font=myFont, command=lambda: rename_table(
        table, rename_table_entry.get())).grid(row=6, columnspan=2, pady=10)


def alter(table):
    for ele in root.winfo_children():
        ele.destroy()

    root.rowconfigure(0, weight=0)

    alter_frame = Frame(root)
    alter_frame.grid(row=4, columnspan=2, sticky="nesw")

    main_lbl = Label(root, text='How would you like to alter the table?',
                     font=myFont).grid(row=0, columnspan=2)

    option_1_btn = Button(root, text='Add', font=myFont, command=lambda: add_columns_selection(
        table, alter_frame)).grid(row=1, column=0, pady=5)
    option_1_lbl = Label(root, text='Add new columns',
                         font=myFont).grid(row=1, column=1)

    option_2_btn = Button(root, text='Rename', font=myFont, command=lambda: rename_columns_selection(
        table, alter_frame)).grid(row=2, column=0, pady=5)
    option_2_lbl = Label(root, text='Rename columns',
                         font=myFont).grid(row=2, column=1)

    option_3_btn = Button(root, text='Rename', font=myFont, command=lambda: rename_table_selection(
        table, alter_frame)).grid(row=3, column=0, pady=5)
    option_3_lbl = Label(root, text='Rename the table',
                         font=myFont).grid(row=3, column=1)


def create(table):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to create').grid(row=0, column=0)


def delete(table):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to delete').grid(row=0, column=0)


def insert(table):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to insert').grid(row=0, column=0)


def query(table):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to query').grid(row=0, column=0)


def save(table):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to save').grid(row=0, column=0)


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
