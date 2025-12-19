import pendulum
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from utils.extract_fyahoo_api import extract_dividends_data
from utils.postgres_utils import load_to_postgres
from utils.alert.email_alert import dag_failure_alert, dag_success_alert, dag_execute_callback

# Environment Variables (set in docker-compose.yaml or Airflow Variables)
POSTGRES_ROOT_USERNAME=Variable.get("POSTGRES_ROOT_USERNAME")
POSTGRES_ROOT_PASSWORD=Variable.get("POSTGRES_ROOT_PASSWORD")

# Default arguments for the DAG
default_args = {
    'owner': 'Jumponpatha | Data Engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['jumponpat59@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': None,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=5)
}

# DAG definition
@dag(
    dag_id='etl_dividend_hist_dag',
    schedule= '@monthly', # Monthly schedule
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["Data Warehouse", "Postgres", "ETL", "Dividened History", "Yahoo Finance"],
    default_args=default_args,
    on_failure_callback=[dag_failure_alert] # Add custom alerts
)

# Function to define the ETL pipeline DAG
def etl_pipeline_dag():
    @task
    def extract_dividend_data_task(symbol: str):
        ''' Extract dividend data for a given stock symbol '''
        ingested_time = datetime.now(ZoneInfo("Asia/Bangkok"))
        extracted_df = extract_dividends_data(symbol)
        extracted_df["Ingested_Time"] = ingested_time
        print(f"Extracted DataFrame:\n {extracted_df.head()}")
        return extracted_df

    @task
    def transform_dividend_data_task(df, symbol):
        ''' Transform the extracted dividend data '''
        print(f"Transforming DataFrame with {len(df)} rows.")
        df["Symbol"] = symbol # Add Columns
        return df

    @task
    def load_dividend_data_task(df):
        ''' Load the transformed data into PostgreSQL (Data Warehouse) '''
        table_name = "finance_stock_dividend_history"
        schema = "raw_finance_stock"
        load_to_postgres(df, table_name, schema, if_exists="replace") # Use 'replace' for demo; consider 'append' for production

    # Call the ETL task
    start = EmptyOperator(task_id="start")
    extract_data = extract_dividend_data_task("AAPL")
    transformed_data = transform_dividend_data_task(extract_data, "AAPL")
    loads_task = load_dividend_data_task(transformed_data)
    end = EmptyOperator(task_id="end")

    # Define task dependencies
    start >> extract_data >> transformed_data >> loads_task >> end

# Generate the DAG
etl_pipeline_dag()