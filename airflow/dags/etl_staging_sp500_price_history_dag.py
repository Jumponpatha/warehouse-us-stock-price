import pendulum
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from utils.postgres_utils import load_to_postgres, query_data_postgres
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
        print(f" Extracting S&P 500 price history data from source database... ")
        query = """
            SELECT * FROM raw_finance_stock.finance_stock_sp500_price_hist;
            """
        extracted_df = query_data_postgres(query)
        return extracted_df

    @task
    def load_dividend_data_task(df):
        ''' Load the transformed data into PostgreSQL (Data Warehouse) '''
        table_name = "finance_stock_sp500_price_hist"
        schema = "raw_finance_stock"
        conn_string = f"postgresql://{POSTGRES_ROOT_USERNAME}:{POSTGRES_ROOT_PASSWORD}@postgres-warehouse:5432/financial_stock_dw"
        load_to_postgres(df, table_name, schema, conn_string, if_exists="replace") # Use 'replace' for demo; consider 'append' for production

    # Call the ETL task
    start = EmptyOperator(task_id="start")
    extract_data = extract_sp500_price_data_from_db_task()
    # loads_task = load_dividend_data_task(extract_data)
    end = EmptyOperator(task_id="end")

    # Define task dependencies
    start >> extract_data >> end

# Generate the DAG
etl_pipeline_dag()