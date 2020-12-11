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

for i in range(3):
    frame.columnconfigure(i, weight=1)

frame.rowconfigure(1, weight=1)

myFont = font.Font(family='Helvetica')

current_time = str(dt.datetime.now().strftime('%d/%m/%Y %H:%M'))
intro_lbl_text = '\n' + current_time + '\n'
intro_lbl_text += 75 * '=' + '\n'
intro_lbl_text += 'Welcome to Python for Postgres! \n'

intro_lbl = Label(root, text=intro_lbl_text, font=myFont).grid(
    row=0, column=0)


def get_table_name():
    table_name = table_entry.get()
    print(table_name)


table_entry_lbl = Label(
    frame, text='SQL table name:    ', font=myFont)
table_entry_lbl.grid(row=1, column=0)
table_entry = Entry(frame, width=30)
table_entry.grid(row=1, column=1)
table_entry_submit = Button(
    frame, text='Submit', font=myFont, command=get_table_name)
table_entry_submit.grid(row=2, columnspan=2, pady=10, ipadx=5, ipady=5)

root.mainloop()
