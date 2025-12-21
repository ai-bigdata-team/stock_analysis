from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_producer_dag',
    default_args=default_args,
    description='DAG to run StockProducer',
    schedule='0 0 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['stock', 'producer'],
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "Stock Producer started at $(date)"',
    )

    run_producer = BashOperator(
        task_id='run_stock_producer',
        bash_command='cd /opt/airflow && python -m dags.collect_data.StockProducer',
    )

    end_task = BashOperator(
        task_id='end_task',
        bash_command='echo "Stock Producer finished at $(date)"',
    )

    start_task >> run_producer >> end_task
