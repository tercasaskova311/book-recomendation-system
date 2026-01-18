#Runs independently at 1 AM daily
import yaml
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import yaml
with('/opt/airflow/config/pipeline_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='new_books',
    description='adding new books from google books api',
    schedule_interval='0 1 * * *',  # 1:00 AM daily
    start_date=days_ago(1),
    catchup=False,  # Don't backfill missed runs
    max_active_runs=1,  # Only one pipeline instance at a time
    tags=['batch'],
) as dag:
    enrich_books_task = BashOperator(
        task_id= 'enrich_books',
        bash_command= '/opt/ingestion/ingestion',
    )
