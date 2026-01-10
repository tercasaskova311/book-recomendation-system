<<<<<<< HEAD
"""
Book Recommendation System - Daily Pipeline
Runs nightly at 2:00 AM to:
1. Generate content-based features (TF-IDF, categories)
2. Train ALS collaborative filtering model
3. Compute hybrid recommendations
"""

=======
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


>>>>>>> 83e4b3c (adjusting makefiel logic + debuging dockerfiles)
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# ============ DAG CONFIGURATION ============
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,  # Each run is independent
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Retry failed tasks twice
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'book_recommendations_pipeline',
    default_args=default_args,
    description='Generate personalized book recommendations',
    schedule_interval='0 2 * * *',  # Cron: 2:00 AM daily
    catchup=False,  # Don't backfill missed runs
    max_active_runs=1,  # Only one pipeline instance at a time
    tags=['ml', 'recommendations', 'spark'],
)

# ============ HELPER FUNCTIONS ============

def check_new_data(**context):
    """
    Check if we have enough new ratings to trigger retraining.
    This prevents unnecessary model updates.
    """
    import psycopg2
    import os
    
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'postgres'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'book_recommendations'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    
    cursor = conn.cursor()
    
    # Count new ratings since last pipeline run
    cursor.execute("""
        SELECT COUNT(*) 
        FROM ratings 
        WHERE timestamp > NOW() - INTERVAL '1 day'
    """)
    
    new_ratings = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    logging.info(f"Found {new_ratings} new ratings in last 24 hours")
    
    # Push to XCom so downstream tasks can access this value
    context['task_instance'].xcom_push(key='new_ratings_count', value=new_ratings)
    
    # Threshold: only retrain if > 100 new ratings
    if new_ratings < 100:
        return False
    
    return True


def validate_pipeline_output(**context):
    """
    Sanity check: ensure recommendations were actually generated
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("ValidationCheck") \
        .getOrCreate()
    
    try:
        # Check if Delta tables exist and have data
        recs_df = spark.read.format("delta").load("delta/final_recommendations")
        count = recs_df.count()
        
        logging.info(f"✅ Found {count:,} recommendations in Delta Lake")
        
        if count < 1000:  # We should have at least 1000 recs
            raise ValueError(f"Too few recommendations generated: {count}")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Validation failed: {e}")
        raise
    finally:
        spark.stop()


# ============ TASK 1: Data Health Check ============
check_data = PythonOperator(
    task_id='check_new_data',
    python_callable=check_new_data,
    provide_context=True,
    dag=dag,
)

# ============ TASK 2: Content-Based Features ============
# This runs your spark/content_similarity.py script

content_features = SparkSubmitOperator(
    task_id='generate_content_features',
    application='/opt/project/spark/content_similarity.py',
    conn_id='spark_default',  # Configured in Airflow connections
    
    # Spark configuration
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '4g',
        'spark.executor.cores': '2',
    },
    
    # Pass environment variables to Spark
    env_vars={
        'DB_HOST': 'postgres',
        'DB_PORT': '5432',
        'PYTHONPATH': '/opt/project',
    },
    
    # Add PostgreSQL JAR for database access
    jars='/opt/project/postgresql-42.7.1.jar',
    
    # Dependencies (install if needed in Spark image)
    packages='io.delta:delta-core_2.12:2.4.0',
    
    dag=dag,
)

# ============ TASK 3: Collaborative Filtering (ALS) ============

collaborative_filtering = SparkSubmitOperator(
    task_id='train_als_model',
    application='/opt/project/spark/user_preferences.py',
    conn_id='spark_default',
    
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '4g',
        'spark.sql.shuffle.partitions': '50',
    },
    
    env_vars={
        'DB_HOST': 'postgres',
        'DB_PORT': '5432',
        'PYTHONPATH': '/opt/project',
    },
    
    jars='/opt/project/postgresql-42.7.1.jar',
    packages='io.delta:delta-core_2.12:2.4.0',
    
    dag=dag,
)

# ============ TASK 4: Hybrid Recommendations ============

hybrid_recommendations = SparkSubmitOperator(
    task_id='generate_hybrid_recommendations',
    application='/opt/project/spark/hybrid_recs.py',
    conn_id='spark_default',
    
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '4g',
    },
    
    env_vars={
        'DB_HOST': 'postgres',
        'DB_PORT': '5432',
        'PYTHONPATH': '/opt/project',
    },
    
    jars='/opt/project/postgresql-42.7.1.jar',
    packages='io.delta:delta-core_2.12:2.4.0',
    
    dag=dag,
)

# ============ TASK 5: Validation ============

validate = PythonOperator(
    task_id='validate_pipeline_output',
    python_callable=validate_pipeline_output,
    provide_context=True,
    dag=dag,
)

# ============ TASK DEPENDENCIES ============
# Define execution order (directed acyclic graph)

# Step 1: Check if we have new data
# Step 2: If yes, run content features and ALS in parallel (they're independent)
# Step 3: Wait for both to finish, then generate hybrid recs
# Step 4: Validate output

check_data >> [content_features, collaborative_filtering] >> hybrid_recommendations >> validate

# This creates:
#         check_data
#         /         \
# content_features  collaborative_filtering
#         \         /
#      hybrid_recs
#           |
#       validate