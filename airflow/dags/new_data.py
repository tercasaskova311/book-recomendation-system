from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import yaml
import sys
import os

# Add project root to Python path
project_root = '/opt/project'
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.enriching_books import enrich_books_batch

config_path = '/opt/project/pipeline_config.yaml'
try:
    with open(config_path) as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    print(f"WARNING: Config file not found at {config_path}, using defaults")
    config = {'new_books_interval': '@daily'}

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
