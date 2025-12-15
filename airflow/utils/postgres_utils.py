from sqlalchemy import create_engine
import psycopg2
import pandas as pd

# Correct connection string for SQLAlchemy
conn_string = "postgresql://bigdata_jchai:bigdata_password8075jcci@postgres-warehouse:5432/financial_stock_dw"

# Create SQLAlchemy engine
db = create_engine(conn_string)
psql_conn = db.connect()

# Psycopg2 connection (still fine)
conn = psycopg2.connect(
    dbname="financial_stock_dw",
    user="bigdata_jchai",
    password="bigdata_password8075jcci",
    host="postgres-warehouse",
    port="5432"
)

conn.autocommit = True
cursor = conn.cursor()

def load_to_postgres(df: pd.DataFrame, table_name, schema, conn_string, if_exists):
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

    if conn_string is None:
        raise ValueError("Connection string must be provided")

    try:
        engine = create_engine(conn_string)
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
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
        df = pd.read_sql_query(query, psql_conn)
        return df
    except Exception as e:
        print(f"Query failed: {e}", exc_info=True)
        raise