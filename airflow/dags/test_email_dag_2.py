from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime
import tempfile, os

def attach_dag_log(dag_run):
    """
    Attach all task logs for the DAG run into a single temporary file.
    """
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".log")
    log_content = ""

    for ti in dag_run.get_task_instances():
        log_content += f"--- Task: {ti.task_id} ---\n"
        log_file_path = ti.log.handlers[0].baseFilename if ti.log.handlers else None
        if log_file_path and os.path.exists(log_file_path):
            with open(log_file_path, "r") as f:
                log_content += f.read() + "\n\n"
        else:
            log_content += "Log file not found.\n\n"

    tmp_file.write(log_content.encode("utf-8"))
    tmp_file.close()
    return tmp_file.name

def dag_success_email_callback(context):
    dag_run = context["dag_run"]
    execution_time = context.get("ts")

    subject = f"[SUCCESS] DAG {dag_run.dag_id} Completed"
    html_content = f"""
    <html>
    <body>
    <h2>DAG Execution Succeeded</h2>
    <p><b>DAG ID:</b> {dag_run.dag_id}</p>
    <p><b>Execution Date:</b> {execution_time}</p>
    <p>The DAG run has completed successfully. Logs are attached.</p>
    </body>
    </html>
    """

    # log_file = attach_dag_log(dag_run)

    send_email(
        to=["jumponpat59@gmail.com"],
        subject=subject,
        html_content=html_content,
        files=[log_file]
    )

with DAG(
    dag_id="email_example_2",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=dag_success_email_callback
) as dag:

    task1 = PythonOperator(
        task_id="task1",
        python_callable=lambda: print("Hello")
    )

    task2 = PythonOperator(
        task_id="task2",
        python_callable=lambda: print("World")
    )

    task1 >> task2
