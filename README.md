# python_for_Postgres
python_for_postgres uses the psycopg2 library to establish a connection to PostgreSQL, providing various SQL database handling functions which are automated by Python.

## Usage
Simply alter the credentials in config.py to have your postgres username, password and database name and run main.py. When prompted, input the name of the SQL table you would like to manipulate/create and you will be greeted by the following menu:

``` a --> Alter existing table
  
  c --> Create new table 
  
  d --> Delete stuff
  
  i --> Insert rows
  
  q --> Query data 
  
  s --> Save the table as a CSV 
  
  u --> Update rows 
  
  Please select one of the above options for what you would like to do:   
