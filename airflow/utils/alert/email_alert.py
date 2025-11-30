# Airflow DAG Alerting Enhancements

# The following changes enhance the ETL DAG with advanced alerting features:


def dag_execute_callback(context):
    print(f"Task has begun execution, task_instance_key_str: {context['task_instance_key_str']}")


def dag_failure_alert(context):
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


def dag_success_alert(context):
    print(f"Dag has succeeded, run_id: {context['run_id']}")