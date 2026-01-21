from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import yaml

# Load pipeline configuration
with open('/opt/airflow/config/pipeline_config.yaml') as f:
    config = yaml.safe_load(f)

# DAG definition
with DAG(
    dag_id='recommendation',
    schedule_interval=config['recommendation_interval'],
    start_date=days_ago(1),
    catchup=False,
    tags=['recs'],
    description='Generate book recommendations using content-based and collaborative filtering'
) as dag:

    # ------------------- Task 1: Content Features -------------------
    content_features = SparkSubmitOperator(
        task_id="generate_content_features",
        application="/opt/project/spark/content_similarity.py",
        name="content_features",
        conf={
            "spark.master": "local[*]",  # Move master into conf
            "spark.sql.shuffle.partitions": "200",
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
        },
        packages="io.delta:delta-core_2.12:2.4.0",  # Delta Lake
        jars="/opt/project/jars/postgresql-42.6.0.jar",  # PostgreSQL JDBC
        verbose=True
    )

    # ------------------- Task 2: Collaborative Filtering -------------------
    collaborative_filtering = SparkSubmitOperator(  # Fixed: Changed variable name
        task_id="generate_collaborative_filtering",  # Fixed: Unique task_id
        application="/opt/project/spark/user_preferences.py",  # Fixed: Correct script
        name="collaborative_filtering",
        conf={
            "spark.master": "local[*]",
            "spark.sql.shuffle.partitions": "200",
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
        },
        packages="io.delta:delta-core_2.12:2.4.0",
        jars="/opt/project/jars/postgresql-42.6.0.jar",
        verbose=True
    )

    # ------------------- Task 3: Hybrid Recommendations -------------------
    hybrid_recommendations = SparkSubmitOperator(
        task_id='generate_hybrid_recs',
        application='/opt/project/spark/hybrid_recs.py',
        name="generate_hybrid_recs",
        conf={
            "spark.master": "local[*]",  # Move master into conf
            "spark.sql.shuffle.partitions": "200",
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
        },
        jars="/opt/project/jars/postgresql-42.6.0.jar",
        verbose=True
    )

    # Task dependencies
    content_features >> collaborative_filtering >> hybrid_recommendations