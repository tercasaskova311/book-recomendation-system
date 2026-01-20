from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import yaml

with open('/opt/airflow/config/pipeline_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id='recommendation',
    schedule_interval=config['recommendation_interval'],
    start_date=days_ago(1),
    catchup=False,
    tags=['recs'],
) as dag:

    content_features = SparkSubmitOperator(
        task_id='generate_content_features',
        application='/opt/project/spark/content_similarity.py',
        conn_id='spark_default',
    )
    collaborative_filtering = SparkSubmitOperator(
    task_id='train_als_model',
    application='/opt/project/spark/user_preferences.py',
    conn_id='spark_default',
    )

    hybrid_recommendations = SparkSubmitOperator(
        task_id='generate_hybrid_recs',
        application='/opt/project/spark/hybrid_recs.py',
        conn_id='spark_default',
    )

    content_features >> collaborative_filtering >> hybrid_recommendations
