from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
import os
import yaml

with open('/opt/airflow/config/pipeline_config.yaml') as f:
    config = yaml.safe_load(f)

from config import SPARK

SPARK_ENV = {
    'spark.master' : SPARK_MASTER,
    'spark.driver.memory' : SPARK_DRIVER_MEMORY,
    'spark.executor.memory': '4g',
    'spark.executor.cores': '2',
    'spark.sql.shuffle.partitions': '50',
}

SPARK_ENV = {
    'DB_HOST': 'postgres',
    'DB_PORT': '5432',
    'PYTHONPATH': '/opt/project',
}

SPARK_JARS = '/opt/project/postgresql-42.7.1.jar'
SPARK_PACKAGES = 'io.delta:delta-core_2.12:2.4.0'

default_args = {
    'owner': 'ml-team',
    'retrieves': 2,
    'retry_delay': timedelta(minutes=5)
}



def should_run_pipeline():
    """
    Check if we have enough new data to retrain.
    Returns True if threshold met, False otherwise.
    """
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'postgres'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'book_recommendations'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD', '')
    )
    cursor = conn.cursor()
    
    # Count new data
    cursor.execute("""
        SELECT 
            (SELECT COUNT(*) FROM ratings WHERE timestamp > NOW() - INTERVAL '1 day') as new_ratings,
            (SELECT COUNT(*) FROM books WHERE created_at > NOW() - INTERVAL '1 day') as new_books
    """)
    
    new_ratings, new_books = cursor.fetchone()
    cursor.close()
    conn.close()
    
    # Check thresholds
    threshold_met = (
        new_ratings >= config.get('new_ratings_threshold', 1000) or
        new_books >= config.get('new_books_threshold', 100)
    )
    
    print(f"📊 New ratings: {new_ratings}")
    print(f"📚 New books: {new_books}")
    print(f"{'✅ Threshold MET' if threshold_met else '⏸️ Threshold NOT met'}")
    
    return threshold_met




with DAG(
    dag_id='recommendation',
    description='ml pipeline fro book recs'
    start_date = days_ago(1),
    schedule_interval= '0 2 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['content similarity', 'user recs'],
) as dag:

    check_threshold = ShortCircuitOperator(
        task_id='check_threshold',
        python_callable=should_run_pipeline,
    )

    content_features = SparkSubmitOperator(
        task_id='generate_content_features',
        application='/opt/project/spark/content_similarity.py',
        conn_id='spark_default',  # Configured in Airflow connections
        conf=SPARK_CONF,
        env_vars=SPARK_ENV,        
        jars=SPARK_JARS,
        packages=SPARK_PACKAGES,
        verbose=True,  
    )

    # ============ TASK 3: Collaborative Filtering (ALS) ============

    collaborative_filtering = SparkSubmitOperator(
        task_id='train_als_model',
        application='/opt/project/spark/user_preferences.py',
        conn_id='spark_default',
        conf=SPARK_CONF,
        env_vars=SPARK_ENV,        
        jars=SPARK_JARS,
        packages=SPARK_PACKAGES,
        verbose=True,  
    )

    # ============ TASK 4: Hybrid Recommendations ============

    hybrid_recommendations = SparkSubmitOperator(
        task_id='generate_hybrid_recommendations',
        application='/opt/project/spark/hybrid_recs.py',
        conn_id='spark_default',
        conf=SPARK_CONF,
        env_vars=SPARK_ENV,        
        jars=SPARK_JARS,
        packages=SPARK_PACKAGES,
        verbose=True,  
    )

    # Gate first
    check_threshold >> [content_features, collaborative_filtering]
    
    # Then combine
    [content_features, collaborative_filtering] >> hybrid_recs




