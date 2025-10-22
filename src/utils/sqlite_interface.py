import sqlite3
import pandas as pd
from typing import Optional

def create_csv_table(csv_filepath: str, db_filepath: str, table_name: str, overwrite: Optional[bool] = False) -> None:
    try:
        df = pd.read_csv(csv_filepath)
        print(f"CSV file '{csv_filepath}' located successfully.")
    except FileNotFoundError:
        print(f"Error: The file '{csv_filepath}' was not found.")
        return
    
    connection = sqlite3.connect(db_filepath)
    print(f"Connected to SQLite database at '{db_filepath}'.")

    if overwrite:
        df.to_sql(table_name, connection, if_exists='replace', index=False)
        print(f"Data loaded into table '{table_name}' successfully.")
    else:
        try:
            df.to_sql(table_name, connection, index=False)
            print(f"Data loaded into table '{table_name}' successfully.")
        except ValueError:
            print(f"'{table_name}' table already exists. Data loading failed.")
    
    cursor = connection.cursor()
    results = cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;").fetchall()

    print(f"First 5 rows of '{table_name}':")
    for row in results:
        print(row)
    
    connection.close()

def load_pandas_to_table(data: pd.DataFrame | pd.Series, db_filepath: str, table_name: str) -> None:
    connection = sqlite3.connect(db_filepath)
    print(f"Connected to SQLite database at '{db_filepath}'.")

    save_index = isinstance(data, pd.Series)
    nrows = data.to_sql(table_name, connection, if_exists='append', index=save_index)
    print(f"Added {nrows} entries to '{table_name}'.")

    connection.close()

def query_db(sql_statement: str, db_filepath: str) -> Optional[list]:
    connection = sqlite3.connect(db_filepath)
    print(f"Connected to SQLite database at '{db_filepath}'.")

    cursor = connection.cursor()
    results = cursor.execute(sql_statement).fetchall()

    connection.close()

    if results is not None and len(results) > 0:
        print(f"Query returned {len(results)} results.")
        return results

def execute_sql_script(sql_script_filepath: str, db_filepath: str) -> Optional[list]:
    with open(sql_script_filepath, 'r') as file:
        sql_script = file.read()
        print(f"SQL script '{sql_script_filepath}' loaded successfully.")
    
    connection = sqlite3.connect(db_filepath)
    print(f"Connected to SQLite database at '{db_filepath}'.")

    cursor = connection.cursor()
    results = cursor.executescript(sql_script).fetchall()

    connection.close()
    
    if results is not None and len(results) > 0:
        return results

def print_tables(db_filepath: str) -> None:
    sql_query = "SELECT name FROM sqlite_schema WHERE type='table';"
    tables = query_db(sql_query, db_filepath)

    print("Tables found:")
    for table in tables:
        print(table[0])

def delete_table(db_filepath: str, table_name: str) -> None:
    delete_statement = f"DROP TABLE IF EXISTS {table_name};"
    query_db(delete_statement, db_filepath)
    print(f"Table '{table_name}' deleted successfully if it existed.")