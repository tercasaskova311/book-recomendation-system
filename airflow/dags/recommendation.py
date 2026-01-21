from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import yaml

# Load pipeline configuration
with open('/opt/airflow/config/pipeline_config.yaml') as f:
    config = yaml.safe_load(f)

# Environment variables for scripts
env_vars = {
    'DB_HOST': 'book-recs-postgres',
    'DB_PORT': '5433',
    'DB_NAME': 'book_recommendations',
    'PYTHONPATH': '/opt/project',
}

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
    content_features = BashOperator(
        task_id="generate_content_features",
        bash_command="""
        cd /opt/project && \
        python spark/content_similarity.py
        """,
        env=env_vars
    )

    # ------------------- Task 2: Collaborative Filtering -------------------
    collaborative_filtering = BashOperator(
        task_id="generate_collaborative_filtering",
        bash_command="""
        cd /opt/project && \
        python spark/user_preferences.py
        """,
        env=env_vars
    )

    # ------------------- Task 3: Hybrid Recommendations -------------------
    hybrid_recommendations = BashOperator(
        task_id='generate_hybrid_recs',
        bash_command="""
        cd /opt/project && \
        python spark/hybrid_recs.py
        """,
        env=env_vars
    )

    # Task dependencies
    content_features >> collaborative_filtering >> hybrid_recommendations