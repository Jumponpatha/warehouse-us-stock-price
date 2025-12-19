import pendulum
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from utils.postgres_utils import load_to_postgres, query_data_postgres, ddl_sql_postgres, table_exists
from utils.alert.email_alert import dag_failure_alert, dag_success_alert, dag_execute_callback

# Default arguments for the DAG
default_args = {
    'owner': 'Jumponpatha | Data Engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['jumponpat59@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=5)
}

# DAG definition
@dag(
    dag_id='etl_staging_sp500_price_history_dag',
    schedule= '@daily', # Monthly schedule
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["Data Warehouse", "Postgres", "Staging", "ETL", "S&P500","Price", "History", "Yahoo Finance"],
    default_args=default_args,
    on_failure_callback=[dag_failure_alert] # Add custom alerts
)

# Function to define the ETL pipeline DAG
def etl_pipeline_dag():
    @task
    def extract_sp500_price_data_from_db_task():
        ''' Extract S&P 500 profile data from database '''
        print(f" Extracting S&P 500 price history data from raw zone ... ")
        query = """
                SELECT "Date", "Open", "High", "Low",
                        "Close", "Volume", "Dividends",
                        "Stock Splits", "Symbol", "Ingested_Time"
                FROM raw_finance_stock.finance_stock_sp500_price_hist;
            """
        extracted_df = query_data_postgres(query)
        return extracted_df

    def ddl_sp500_price_history_staging_task(table_name: str, schema: str):
        ''' Create staging table if not exists '''
        print(f" Creating staging table if not exists ... ")

        ddl_statement = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name}
                (
                    EVENT_ID SERIAL PRIMARY KEY,
                    SYMBOL VARCHAR(256),
                    DATE TIMESTAMPTZ,
                    OPEN VARCHAR(256),
                    HIGH VARCHAR(256),
                    LOW VARCHAR(256),
                    CLOSE VARCHAR(256),
                    VOLUME VARCHAR(256),
                    DIVIDENDS VARCHAR(256),
                    STOCK_SPLITS VARCHAR(256),
                    PROCESSED_TIME TIMESTAMPTZ,
                    INGESTED_TIME TIMESTAMPTZ
                );
        """
        _ = ddl_sql_postgres(ddl_statement)
        return
    @task
    def transform_sp500_price_data_task(df):
        ''' Transform the extracted S&P 500 price history data '''

        # Add Processed Time
        print("Adding Processed Time and transforming data types ...")
        processed_time = datetime.now(ZoneInfo("Asia/Bangkok"))
        df["Processed_Time"] = processed_time
        # Add Data Types
        df["Volume"] = df["Volume"].astype("int64")
        df["Dividends"] = df["Dividends"].astype("float64")
        df["Stock Splits"] = df["Stock Splits"].astype("float64")

        # Drop Duplicates
        print("Removing Duplicates ...")
        df = df.drop_duplicates()

        # NULL Handling
        print("Handling NULL values ...")
        transformed_df = df.dropna(subset=["Date", "Close", "Symbol"])

        print(f"Successfully transforming dataframe with {len(df)} rows.")
        return transformed_df

    @task
    def load_dividend_data_task(df):
        ''' Load the transformed data into PostgreSQL (Data Warehouse) '''
        table_name = "staging_finance_stock_sp500_price_hist"
        schema = "staging_finance_stock"
        if table_exists(table_name, schema):
            print("Table exists")
        else:
            print("Table does not exist")
            ddl_sp500_price_history_staging_task(table_name, schema)
        table_name = "staging_finance_stock_sp500_price_hist"
        schema = "staging_finance_stock"
        load_to_postgres(df, table_name, schema, if_exists="replace") # Use 'replace' for demo; consider 'append' for production

    # Call the ETL task
    start = EmptyOperator(task_id="start")
    extract_data = extract_sp500_price_data_from_db_task()
    transform_data = transform_sp500_price_data_task(extract_data)
    loads_task = load_dividend_data_task(transform_data)
    end = EmptyOperator(task_id="end")

    # Define task dependencies
    start >> extract_data >> transform_data >> loads_task >> end
# Generate the DAG
etl_pipeline_dag()