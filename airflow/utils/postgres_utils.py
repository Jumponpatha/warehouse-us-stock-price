import pandas as pd
from sqlalchemy import text
import psycopg2
from airflow.sdk import Variable
from sqlalchemy import create_engine, inspect,  MetaData, Table
from sqlalchemy.dialects.postgresql import insert

# Create PostgreSQL Connection
def get_postgres_connection():
    """
    Establish a connection to the PostgreSQL database using SQLAlchemy.
    """

    # Environment Variables (set in docker-compose.yaml or Airflow Variables)
    POSTGRES_ROOT_USERNAME = Variable.get("POSTGRES_ROOT_USERNAME")
    POSTGRES_ROOT_PASSWORD = Variable.get("POSTGRES_ROOT_PASSWORD")

    POSTGRES_HOST = "postgres-warehouse"
    POSTGRES_PORT = 5432
    POSTGRES_DB = "financial_stock_dw"

    # Correct connection string for SQLAlchemy
    conn_string = (
        f"postgresql+psycopg2://{POSTGRES_ROOT_USERNAME}:"
        f"{POSTGRES_ROOT_PASSWORD}@{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    # Create SQLAlchemy engine
    return create_engine(conn_string, pool_pre_ping=True)


def load_to_postgres(df: pd.DataFrame, table_name, schema, if_exists):
    """
    Load a Pandas DataFrame into a PostgreSQL table using SQLAlchemy.

    Parameters:
        df (pd.DataFrame): DataFrame to load
        table_name (str): Name of the target table
        schema (str): PostgreSQL schema (default: public)
        conn_string (str): SQLAlchemy connection string
        if_exists (str): What to do if table exists: 'fail', 'replace', 'append'
    """

    if df is None or df.empty:
        print(f"No data to load into {table_name}. Skipping.")
        return

    try:
        engine = get_postgres_connection()
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=50_000,
            method='multi'  # batch insert for performance
        )
        print(f"Loaded {len(df)} rows into {schema}.{table_name} successfully.")

    except Exception as e:
        print(f"Failed to load data into {schema}.{table_name}: {e}", exc_info=True)
        raise

def query_data_postgres(query: str) -> pd.DataFrame:
    """
    Execute a SQL query and return the results as a Pandas DataFrame.

    Parameters:
        query (str): SQL query to execute
    Returns:
        pd.DataFrame: Query results as DataFrame
    """

    try:
        engine = get_postgres_connection()
        df = pd.read_sql_query(query, engine)
        print(f"The data was successfully queried.")
        print(f"DataFrame Shape: {df.shape}")
        return df
    except Exception as e:
        print(f"Query failed: {e}", exc_info=True)
        raise

def read_data_postgres(table_name: str, schema: str) -> pd.DataFrame:
    """
    Execute a SQL query and return the results as a list of tuples.

    Parameters:
        query (str): SQL query to execute
    """

    try:
        engine = get_postgres_connection()
        df = pd.read_sql_table(
                table_name=table_name,
                con=engine,
                schema=schema, # change if needed
                chunksize=50_000,
                index=False,
                method="multi"
            )

        print(f"The data was successfully read.")
        print(f"The {table_name} table has {df.shape[0]} rows and {df.shape[1]} columns.")
        return df
    except Exception as e:
        print(f"Failed to read data from {schema}.{table_name}: {e}", exc_info=True)
        raise

def ddl_sql_postgres(ddl_statement: str):
    """
    Execute a DDL statement (e.g., CREATE TABLE) in PostgreSQL.

    Parameters:
        ddl_statement (str): DDL SQL statement to execute.
    """

    try:
        engine = get_postgres_connection()
        with engine.connect() as connection:
            connection.execute(text(ddl_statement))
        print("DDL statement executed successfully.")
    except Exception as e:
        print(f"Failed to execute DDL statement: {e}", exc_info=True)
        raise

def table_exists(table_name: str, schema: str) -> bool:
    """
    Docstring for table_exists

    :param engine: Description
    :param table_name: Description
    :type table_name: str
    :param schema: Description
    :type schema: str
    :return: Description
    :rtype: bool
    """
    engine = get_postgres_connection()

    try:
        inspector = inspect(engine)
        exists = inspector.has_table(table_name, schema=schema)
        return exists
    finally:
        engine.dispose()


def insert_if_not_exists(df, table_name, schema):
    engine = get_postgres_connection()
    metadata = MetaData(schema=schema)

    table = Table(table_name, metadata,autoload_with=engine)

    insert_stmt = insert(table).values(df.to_dict(orient="records"))

    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(
        index_elements=["symbol", "date"]
    )

    with engine.begin() as conn:
        conn.execute(do_nothing_stmt)