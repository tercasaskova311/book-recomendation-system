from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
import os
import yaml

with open ('/opt/airflow/config/pipeline_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='recommendation',
    description='ml pipeline for book recs',
    start_date = days_ago(1),
    schedule_interval= config['recommendation'],
    max_active_runs=1,
    catchup=False,
    tags=['recs'],
) as dag:

    content_features = SparkSubmitOperator(
        task_id='generate_content_features',
        application='opt/spark/content_similarity.py',
        conn_id='spark_default',  # Configured in Airflow connections
        packages="io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.master": "local[*]",
            "spark.driver.memory": "4g",
            "spark.driver.memoryOverhead": "1g",
        },
        env_vars={"PYTHONPATH": "/opt/project"},
        verbose=True, 
    )

    collaborative_filtering = SparkSubmitOperator(
        task_id='train_als_model',
        application='/opt/spark/user_preferences.py',
        conn_id='spark_default',
        packages="io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.master": "local[*]",
            "spark.driver.memory": "4g",
            "spark.driver.memoryOverhead": "1g",
        },
        env_vars={"PYTHONPATH": "/opt/project"},
        verbose=True, 
    )

    hybrid_recommendations = SparkSubmitOperator(
        task_id='generate_hybrid_recommendations',
        application='/opt/spark/hybrid_recs.py',
        conn_id='spark_default',
        packages="io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.master": "local[*]",
            "spark.driver.memory": "4g",
            "spark.driver.memoryOverhead": "1g",
        },
        env_vars={"PYTHONPATH": "/opt/project"},
        verbose=True,
    )
    
    # Gate first
    check_threshold >> [content_features, collaborative_filtering]
    
    # Then combine
    [content_features, collaborative_filtering] >> hybrid_recommendations  




