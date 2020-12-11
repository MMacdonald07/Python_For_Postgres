import time
import pandas as pd
import datetime as dt
from tkinter import *
import tkinter.font as font

from Database_Class import DatabaseConnection

root = Tk()
root.title('Python for Postgres')
root.geometry('400x400')

root.columnconfigure(0, weight=1)
root.rowconfigure(1, weight=1)

frame = Frame(root)
frame.grid(row=1, column=0, sticky='nsew')

for num in range(3):
    frame.columnconfigure(num, weight=1)

frame.rowconfigure(1, weight=1)

myFont = font.Font(family='Helvetica')

current_time = str(dt.datetime.now().strftime('%d/%m/%Y %H:%M'))
intro_lbl_text = '\n' + current_time + '\n'
intro_lbl_text += 75 * '=' + '\n'
intro_lbl_text += 'Welcome to Python for Postgres! \n'

intro_lbl = Label(root, text=intro_lbl_text, font=myFont).grid(
    row=0, column=0)


def alter(database_connection):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to alter').grid(row=0, column=0)
    database_connection.close_connection()


def create(database_connection):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to create').grid(row=0, column=0)
    database_connection.close_connection()


def delete(database_connection):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to delete').grid(row=0, column=0)
    database_connection.close_connection()


def insert(database_connection):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to insert').grid(row=0, column=0)
    database_connection.close_connection()


def query(database_connection):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to query').grid(row=0, column=0)
    database_connection.close_connection()


def save(database_connection):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to save').grid(row=0, column=0)
    database_connection.close_connection()


def update(database_connection):
    for ele in root.winfo_children():
        ele.destroy()
    lbl = Label(root, text='You have chosen to update').grid(row=0, column=0)
    database_connection.close_connection()


def main_program(table):
    for ele in root.winfo_children():
        ele.destroy()

    frmMain = Frame(root)
    frmMain.grid(row=1, column=0, sticky="nesw")

    for i in range(2):
        frmMain.columnconfigure(i, weight=1)

    root.rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)

    database_connection = DatabaseConnection(table)

    main_lbl = Label(root, text='What would you like to do?', font=myFont).grid(
        row=0, column=0)

    alter_btn = Button(frmMain, text='Alter', font=myFont, command=lambda: alter(
        database_connection)).grid(row=1, column=0, pady=5)
    alter_btn_lbl = Label(
        frmMain, text='Alter the existing table', font=myFont).grid(row=1, column=1)

    create_btn = Button(frmMain, text='Create', font=myFont, command=lambda: create(
        database_connection)).grid(row=2, column=0, pady=5)
    create_btn_lbl = Label(
        frmMain, text='Create a new table with this name', font=myFont).grid(row=2, column=1)

    delete_btn = Button(frmMain, text='Delete', font=myFont, command=lambda: delete(
        database_connection)).grid(row=3, column=0, pady=5)
    delete_btn_lbl = Label(
        frmMain, text='Delete stuff in the table', font=myFont).grid(row=3, column=1)

    insert_btn = Button(frmMain, text='Insert', font=myFont, command=lambda: insert(
        database_connection)).grid(row=4, column=0, pady=5)
    insert_btn_lbl = Label(
        frmMain, text='Insert new data into the table', font=myFont).grid(row=4, column=1)

    query_btn = Button(frmMain, text='Query', font=myFont, command=lambda: query(
        database_connection)).grid(row=5, column=0, pady=5)
    query_btn_lbl = Label(
        frmMain, text='Query the data', font=myFont).grid(row=5, column=1)

    save_btn = Button(frmMain, text='Save', font=myFont, command=lambda: save(
        database_connection)).grid(row=6, column=0, pady=5)
    save_btn_lbl = Label(
        frmMain, text='Save the table as a .csv', font=myFont).grid(row=6, column=1)

    update_btn = Button(frmMain, text='Update', font=myFont, command=lambda: update(
        database_connection)).grid(row=7, column=0, pady=5)
    update_btn_lbl = Label(
        frmMain, text='Update the table\'s data', font=myFont).grid(row=7, column=1)


table_entry_lbl = Label(
    frame, text='SQL table name:', font=myFont)
table_entry_lbl.grid(row=1, column=0)
table_entry = Entry(frame, width=30)
table_entry.grid(row=1, column=1)
table_entry_submit = Button(
    frame, text='Submit', font=myFont, command=lambda: main_program(table_entry.get()))
table_entry_submit.grid(row=2, columnspan=2, pady=10, ipadx=5, ipady=5)

root.mainloop()
