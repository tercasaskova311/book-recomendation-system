from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import yaml
import sys

sys.path.insert(0, '/opt/project')
from ingestion.enriching_books import enrich_books_batch


with open('/opt/airflow/config/pipeline_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='enrich_new_books',
    schedule_interval=config['new_books_interval'],
    start_date=days_ago(1),
    catchup=False,
    tags=['enrichment', 'google-api'],
    description='Daily enrichment of unenriched books using Google Books API'
) as dag:

    enrich_task = PythonOperator(
        task_id='enrich_unenriched_books',
        python_callable=enrich_books_batch,
        op_kwargs={'limit': 100}  # Process 100 books per day
    )
