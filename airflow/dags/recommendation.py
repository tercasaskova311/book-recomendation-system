from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import yaml

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='recommendation',
    start_date = days_ago(1),
    schedule_interval=config['recommendation_interval'],
    catchup=False,
    tags=['content similarity', 'user recs'],
) as dag:

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




