import pytest
from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag()

# Test to ensure the DAG is loaded correctly
def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="etl_dividend_hist_dag")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 5