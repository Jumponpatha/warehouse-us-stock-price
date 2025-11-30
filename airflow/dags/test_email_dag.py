from airflow import DAG
from airflow.utils.email import send_email
from airflow.operators.python import PythonOperator
from datetime import datetime
import io
import tempfile
import os
def get_task_log_content(ti):
    log_stream = io.StringIO()
    ti.log.addHandler(log_stream)
    ti.log.info("Collecting task logs")  # optional, ensures handler is active
    return log_stream.getvalue()

def attach_task_log(ti):
    """
    Save the task log to a temp file and return the file path.
    Works with Airflow 3.x and encoded run_id folders.
    """
    # Get log file path from TaskInstance logger
    try:
        log_file_path = ti.log.handlers[0].baseFilename
    except Exception:
        # fallback to manual path
        log_base = os.environ.get("AIRFLOW_HOME", "/opt/airflow") + "/logs"
        safe_run_id = ti.run_id.replace(":", "%3A")
        log_file_path = f"{log_base}/{ti.dag_id}/{ti.task_id}/{safe_run_id}/attempt={ti.try_number}.log"

    if os.path.exists(log_file_path):
        with open(log_file_path, "r") as f:
            log_content = f.read()
    else:
        log_content = "Log file not found."

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".log")
    tmp_file.write(log_content.encode("utf-8"))
    tmp_file.close()
    return tmp_file.name

# SUCCESS EMAIL CALLBACK
def send_success_status_email(context):
    ti = context["ti"]
    status = ti.state
    execution_time = context.get("ts")  # ISO 8601 string
    # Construct local log path
    subject = f"[SUCCESS] Airflow Task {ti.dag_id} "
    html = f"""
    <html>
    <body>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        h2 {{ color: green; }}
        p {{ margin: 4px 0; }}
    </style>

    <h2>Airflow Task Succeeded</h2>
    <p>Hi All,</p>
    <p>The Airflow task has completed successfully. Please find the task details below:</p>


    <p><b>Task:</b> {ti.task_id}</p>
    <p><b>Status:</b> {status}</p>
    <p><b>Execution Date:</b> {execution_time} </p>
    <p><b>Log URL:</b> <a href="{ti.log_url}">View Logs</a></p>

    <p>
    This is an automated notification from the Airflow system. Please do not reply to this email.<br><br>
    Best regards,<br>
    Airflow Data Engineering Team
    </p>
    """
    log_file = attach_task_log(ti)

    send_email(
        to=["jumponpat59@gmail.com"],
        subject=subject,
        html_content=html,
        files=[log_file]
    )


# FAILURE EMAIL CALLBACK
def send_failure_status_email(context):
    ti = context["ti"]
    status = ti.current_state()
    execution_time = context.get("ts")  # ISO 8601 string
    subject = f"[FAILED] Airflow Task {ti.task_id}"
    html = f"""
    <html>
    <body>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        h2 {{ color: red; }}
        p {{ margin: 4px 0; }}
    </style>

    <h2>[ERROR] Airflow Task Failed</h2>

    <p><b>Task:</b> {ti.task_id}</p>
    <p><b>Status:</b> {status}</p>
    <p><b>Execution Date:</b> {execution_time}</p>
    <p><b>Log URL:</b> <a href="{ti.log_url}">View Logs</a></p>

    <p style="color:gray; font-size:12px; margin-top:20px;">This is an automated email from Airflow.</p>
    </body>
    </html>
    """

    send_email(
        to=["jumponpat59@gmail.com"],
        subject=subject,
        html_content=html,
        # from_email="Airflow Alert Email <no-replies@airflow.com>"
    )


# DAG DEFINITION
with DAG(
    dag_id="email_example",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
):

    task_to_watch = PythonOperator(
        task_id="start_task",
        python_callable=lambda: 1
    )

    task_to_watch.on_success_callback = [send_success_status_email]
    task_to_watch.on_failure_callback = [send_failure_status_email]
