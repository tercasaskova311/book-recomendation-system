from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import yaml

with open('/opt/airflow/config/pipeline_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='new_books',
    schedule_interval=config['new_books_interval'],
    start_date=days_ago(1),
    catchup=False,
    tags=['batch'],
) as dag:

    enrich_books = BashOperator(
        task_id='enrich_books',
        bash_command='cd /opt/project && PYTHONPATH=/opt/project python ingestion/ingestion.py',
        env={
            'PYTHONPATH': '/opt/project',
            'GOOGLE_API_KEY': '{{ var.value.GOOGLE_API_KEY }}'
        }
    )
