import re
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import numpy as np
from psycopg2.extensions import register_adapter, AsIs

DB_PARAMS = {
    "host": "localhost",
    "user": "username",
    "database": "covid_db",
    "password": "yourpass"
}

TODAY = datetime.now().date()

YESTERDAY = TODAY - timedelta(days=1)

register_adapter(np.int64, AsIs)


def get_national_14day_covid_data() -> pd.DataFrame:
  """
  Retrieves the JSON data from the provided datasource.

  Returns:
      df_covid: pd.DataFrame with data on 14-day notification rate of new COVID-19 cases and deaths.
  """
  try:
      response = requests.get(
          "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/")
      data = response.json()
      df_covid = pd.DataFrame(data)

  except:
    raise

  return df_covid


def correct_column_name(name: str) -> str:
  """
  Standardizes column names for dataframes, removing spaces and special characters and converting every upper to lower case.

  Args:
      - name: string with the name of the column to be treated.

  Returns:
      str: string with corrected column name.
  """
  name = re.sub(r'[^\w\s]', '', name)
  name = name.lower().strip()
  name = name.replace(" ", "_")

  return name


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
  """
  Corects column names through iterating a loop on every column, calling the standardize_column_name function.

  Args:
      - df: pd.Dataframe with the dataframe to be treated.

  Returns:
      pd.DataFrame: Pandas DataFrame with corrected column names.
  """

  df.columns = [correct_column_name(column) for column in df.columns]

  return df


def transform_phase(df: pd.DataFrame, string_to_float_columns_list=None) -> pd.DataFrame:
  """
  Executes all relevant transformation to the dataframe prior to the load.

  Args:
      - df: pd.Dataframe to be treated.
      - string_to_float_columns_list: list of strings with the names of columns to be treated from string to float types. Standard value is None, in case there are no columns with string to float conversions to be made.

  Returns:
      transformed_df: Pandas DataFrame with final transformations.
  """

  transformed_df = standardize_column_names(df)
  transformed_df['country'] = df['country'].str.strip()
  transformed_df['updated_at'] = TODAY

  return transformed_df


def connect_to_postgres() -> psycopg2.connect:
  """
  This function creates a connection to local PostgreSQL database.

  Args:
      - database: string containaing the name of the database to be connected with. Standard value is None in case the - - connection is not to be made to an specific database, e.g. when creating a new database.

  Returns:
      conn: psycopg2.connection object that contains connection to database.
  """
  conn = psycopg2.connect(**DB_PARAMS)
  conn.autocommit = True

  return conn


def get_database_latest(schema_name: str, table_name: str) -> pd.DataFrame:
  """
  Retrieves the latest updated data from PostgreSQL database.
  Args:
    - schema_name: string containing the schema name of the updated table.
    - table_name:  string containg the table_name for the updated table. 
  
  Returns:
    db_df: dataframe with the latest daa from the PostgreSQL database. 
  """

  sql_query = f"SELECT country, country_code, continent, population, 'indicator', year_week, source, note, weekly_count, cumulative_count, rate_14_day FROM {schema_name}.{table_name} WHERE updated_at = CAST('{YESTERDAY}' AS DATE)"

  try:
      conn = connect_to_postgres()
      db_df = pd.read_sql(sql_query, conn)
  except Exception as e:
      print(f"Erro ao carregar os dados do banco de dados: {str(e)}")

  conn.close()

  return db_df


def search_updates(extracted_df: pd.DataFrame, database_df: pd.DataFrame) -> pd.DataFrame:
  """
  Searches for the rows to update the database based on new extraction.
  
  Args:
    - extracted_df: dataframe from daily extraction from source.
    - database_df: dataframe from database to be updated.
    
  Returns:
    diff_df: dataframe with the rows that will update the database.
  """

  diff_df = extracted_df.merge(database_df, on=["country", "year_week"], how="left", suffixes=('', '_x'))

  # Filters lines that were extracted but are no in the database
  diff_df = diff_df[diff_df.isna().any(axis=1)]

  # Maintains only lines that differ
  extraction_columns = [col for col in diff_df.columns if not col.endswith("_x")]

  # Updates the dataframe to have only the different rows and columns.
  diff_df = diff_df[extraction_columns]

  diff_df = diff_df.drop(columns=['?column?'])

  return diff_df

def upsert_to_database(diff_df: pd.DataFrame, schema_name: str, table_name: str) -> None:
  
  """
  Inserts and updates new and altered rows into the database.
  
  Args: 
    - diff_df: dataframe with rows to be updated or inserted into database.
    - schema_name: name of the schema with the table to be altered.
    - table_name: name of the table to be altered.
  """

  # Converts DataFrame to list of tuples
  data_to_update = [tuple(row) for row in diff_df.to_records(index=False)]

  # Defines sql INSERT INTO ON CONFLICT command
  sql = f"""
  INSERT INTO {schema_name}.{table_name} (country, country_code, continent, population, "indicator", year_week, source, note, weekly_count, cumulative_count, rate_14_day, updated_at)
  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
  ON CONFLICT (country, year_week, "indicator") DO UPDATE
  SET 
      country = EXCLUDED.country,
      country_code = EXCLUDED.country_code,
      continent = EXCLUDED.continent,
      population = EXCLUDED.population,
      "indicator" = EXCLUDED."indicator",
      year_week = EXCLUDED.year_week,
      source = EXCLUDED.source,
      note = EXCLUDED.note,
      weekly_count = EXCLUDED.weekly_count,
      cumulative_count = EXCLUDED.cumulative_count,
      rate_14_day = EXCLUDED.rate_14_day,
      updated_at = EXCLUDED.updated_at
  """

  try:
          # Executar a consulta SQL
      conn = connect_to_postgres()
      cursor = conn.cursor()

      cursor.executemany(sql, data_to_update)
      conn.commit()
  except:
      raise
    
  conn.close()

def main():
  """
  Executes the ETL.
  """
  extract_df = get_national_14day_covid_data()
  transformed_extract_df = transform_phase(extract_df)
  database_df = get_database_latest('covid_data', 'national_14day_notification_rate_covid_19')
  updates_df = search_updates(transformed_extract_df, database_df)
  upsert_to_database(updates_df, 'covid_data', 'national_14day_notification_rate_covid_19')

if __name__ == "__main__":
    main()
