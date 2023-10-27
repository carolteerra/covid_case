import re
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def main() -> None:
  """
  Executes the load pipeline.
  """
  schema_name = 'covid_data'
  table_name= 'covid_vaccination_data'
  primary_key_cols=['iso_code', 'location', 'date']
  vaccine_df = get_covid_vaccine_data()
  create_table_sql = create_sql_script(df=vaccine_df, schema_name=schema_name, table_name=table_name, primary_key_cols=primary_key_cols)
  execute_create_sql_command(object_name=table_name, object_type="table", schema_name=schema_name, create_table_sql=create_table_sql)
  insert_dataframe_to_postgres(df=vaccine_df, schema_name=schema_name, table_name=table_name)

DB_PARAMS = {
  "host": "localhost",
  "user": "username",
  "database": "covid_db",
  "password": "yourpass"
}

def get_covid_vaccine_data() -> pd.DataFrame:
  """
  Uploads the csv file for the datasource.

  Returns:
    df_covid: pd.DataFrame with data on covid-19 vaccination across the globe.
  """
  df_covid = pd.read_csv('owid-covid-data.csv')

  return df_covid

def create_sql_script(df:  pd.DataFrame, table_name: str, schema_name: str, primary_key_cols=None) -> str:
  """
  Creates the script that will be used to create the tables in the database prior to the first load, based on its column types. A primary key for each table can also be defined in this script. 

  Args: 
      - df: dataframe to be inserted into table.
      - table_name: name that the table will have in the PostgreSQL database.
      - schema_name: name of the schema where the table will be set.
      - primary_key_cols: list of column names to be used as table primary key. Can be a list containing a single value.Will be None in case a primary key is not to be set. 

  Returns:
      sql_script: the string containing the sql script with column names and types to create new tables. 
  """

  data_types = {
      "int64": "INTEGER",
      "float64": "NUMERIC",
      "object": "TEXT",
      "datetime64[ns]": "TIMESTAMP",
      "bool": "BOOLEAN"
  }

  # Initializes the SQL script with the CREATE TABLE command
  sql_script = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"

  # Loops through the DataFrame columns
  for column in df.columns:
      # Use TEXT as the default type
      data_type = data_types.get(str(df[column].dtype), "TEXT")
      nullability = "NOT NULL" if df[column].notnull().all() else "NULL"
      sql_script += f"    {column} {data_type} {nullability},\n"

  # Adds the primary key declaration if columns are specified
  if primary_key_cols:
      primary_key = ", ".join(primary_key_cols)
      sql_script += f"    PRIMARY KEY ({primary_key}),\n"

  # Removes the extra comma at the end and close the CREATE TABLE command
  sql_script = sql_script[:-2] + "\n);"
  
  print(sql_script)

  return sql_script

def connect_to_postgres() -> psycopg2.connect:
  """
  Creates a connection to local PostgreSQL database.

  Args: 
      - database: string containaing the name of the database to be connected with. Standard value is None in case the - - connection is not to be made to an specific database, e.g. when creating a new database.

  Returns:
      conn: psycopg2.connection object that contains connection to database.
  """
  conn = psycopg2.connect(**DB_PARAMS)
  conn.autocommit = True
  return conn

def execute_create_sql_command(object_name: str, object_type: str, schema_name=None, create_table_sql=None) -> None:
  """
  This function creates the sql command that will be used to create either a database, schema or table within our PostgreSQL database. It utilizes our connection to PostgreSQL to create the desired object. 

  Args:
      - object_name: string containing the name o the object to be created.
      - object_type: string containing the type of the object to be created. Accepted values are 'database', 'schema' and 'table'.
      - schema_name: optional value. String containing name of the schema where a table is created.
      - create_table_sql: optional value. String containing the SQL script with the CREATE TABLE command for a given table.     
  """

  try:
      if object_type == 'database':
          conn = connect_to_postgres()
          cursor = conn.cursor()
          cursor.execute(f"DROP {object_type} IF EXISTS {object_name};")
          # Execute the SQL command to create the object
          cursor.execute(f"CREATE {object_type} {object_name};")
          print(f"{object_type} '{object_name}' created successfully!")

      elif object_type == 'schema':
          conn = connect_to_postgres()
          cursor = conn.cursor()
          cursor.execute(f"DROP {object_type} IF EXISTS {object_name} CASCADE;")
          cursor.execute(f"CREATE {object_type} IF NOT EXISTS {object_name};")
          print(f"{object_type} '{object_name}' created successfully!")

      elif object_type == 'table':
          conn = connect_to_postgres()
          cursor = conn.cursor()
          cursor.execute(f"DROP {object_type} IF EXISTS {schema_name}.{object_name};")
          cursor.execute(create_table_sql)
          print(f"{object_type} '{schema_name}.{object_name}' created successfully!")

  except (Exception, psycopg2.Error) as error:
      raise error

  finally:
      if conn:
          conn.close()

def insert_dataframe_to_postgres(df: pd.DataFrame, table_name: str, schema_name: str, if_exists='replace') -> None:
  """
  Inserts the treated dataframe into the tables created in PostgreSQL database.

  Args: 
      - df: pd.DataFrame to be inserted.
      - table_name: name of the table in PostgreSQL database
      - schema_name: name of schema containing table in PostgreSQL database
      - if_exists: specifies the behavior if the table already exists. This script is supposed to make one batch ingestion with all existing data on the data sources given, so we choose to replace. Avoid this method for incremental loads. 
  """
  engine = create_engine(
      f'postgresql://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}/{DB_PARAMS["database"]}')

  try:
      df.to_sql(table_name, schema=schema_name, con=engine,
                if_exists=if_exists, index=False)
  except SQLAlchemyError as e:
      raise e
    
if __name__ == "__main__":
    main()
