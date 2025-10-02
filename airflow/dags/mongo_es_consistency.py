from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Python callable
def hello_world():
    print("Hello Airflow! DAG is working correctly.")

# Default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="test_hello_dag",
    default_args=default_args,
    description="Simple test DAG to verify Airflow setup",
    start_date=datetime(2025, 10, 2),
    schedule_interval=None,  # manual trigger
    catchup=False,
    max_active_runs=1,
    tags=["test"],
) as dag:

    test_task = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world,
    )
