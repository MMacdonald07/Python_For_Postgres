# python_for_Postgres
python_for_postgres uses the psycopg2 library to establish a connection to PostgreSQL, providing various SQL database handling functions which are automated by Python.

## Installation

### Clone
Clone this repository to your machine using https://github.com/MMacdonald07/python_for_postgres.git.

### Setup
Use the package manager pip to install requirements so the program can be run:

```bash
pip install -r requirements.txt
```

Download [PostgreSQL](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads) and create an account on the SQL shell (PSQL).  

## Usage
On PSQL, use the following command to make a database:

```bash
user=# CREATE DATABASE dbname
```

Simply input your PSQL credentials in config.py to have your postgres username, password and database name and run main.py. When prompted, input the name of the SQL table you would like to manipulate/create and you will be greeted by the following menu:

```bash
  
  a --> Alter existing table
  
  c --> Create new table 
  
  d --> Delete stuff
  
  i --> Insert rows
  
  q --> Query data 
  
  s --> Save the table as a CSV 
  
  u --> Update rows 
  
  Please select one of the above options for what you would like to do:   
  ```
### Testing
The test_data.csv file contains sample data from [Kaggle](https://www.kaggle.com/stefanoleone992/fifa-20-complete-player-dataset?select=players_20.csv) that can be used in conjunction with python_for_postgres.
 
## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
Please make sure to update tests as appropriate.
