from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import psycopg2
import os
from airflow.operators.python import ShortCircuitOperator

with open('')
    config = yaml.safe_load(f)

default_args =()

with DAG(
    'book_recommendations_pipeline',
    default_args=default_args,
    description='Generate personalized book recommendations',
    schedule_interval='0 2 * * *',  # Cron: 2:00 AM daily
    catchup=False,  # Don't backfill missed runs
    max_active_runs=1,  # Only one pipeline instance at a time
    tags=['ml', 'recommendations', 'spark'],
) as dag:

    #100 books per day from google books api
    download_books = SparkSubmitOperator(
        task_id='download_books',
        application='/opt/project/ingestion/load_data.py',
        conn_id='spark_default',
        conf={'spark.master': 'local[*]'},
        env_vars={
            'GOOGLE_API_KEY': os.getenv('GOOGLE_API_KEY'),
            'DB_HOST': os.getenv('DB_HOST'),
            'DB_PORT': os.getenv('DB_PORT'),
            'DB_USER': os.getenv('DB_USER'),
            'DB_PASSWORD': os.getenv('DB_PASSWORD'),
            'DB_NAME': os.getenv('DB_NAME'),
            'PYTHONPATH': '/opt/project',
        },
        jars='/opt/project/postgresql-42.7.1.jar',
        packages='io.delta:delta-core_2.12:2.4.0',
        dag=dag,
    )

    download_ratings = BashOperator(
        task_id='download_ratings',
        bash_command=''
    )

    process_similarities = SparkSubmitOperator(
        task_id='process_similarities',
        application='',
        name='',
        package=''
        conf={

        }
        env_vars={}
        verbose=True
    )










#Orchestration: Apache Airflow (optional) or Cron = Trigger: Daily at 2:00 AM OR when new_ratings > 1000 
#okay so I have these scripts:
#ingestion/load_data.py = enriching kaggle data by google_books_api (100 per day)- I should schedule that as well
#spark:
"""
- spark/content_similarity.py = fetching features from description, language, pages = computing similarity of vectors by lsh
- spark/user_preferences.py = getting ratings of users from kaggle => computing ALS score - each user top 100 books => idk yet how would this work potentionally in real life scenario - for now I have only static data from kaggle - there is many users tho - I am thinking tho that for a serving layer - I would create a possibility to rate a book(by isbn) and this would be add to the postgres rating csv and depends and add to the system?? idk this is kinda advnace..
- spark/hybrid_recs.py = getting the two recs score together - creating a recommendation based on both user rating of some other book and existing books
"""

#streamlit: app/dashboard.py - simple UI for the scrits - will be adjusted based on the othe scripts







# This creates:
#         check_data
#         /         \
# content_features  collaborative_filtering
#         \         /
#      hybrid_recs
#           |
#       validate