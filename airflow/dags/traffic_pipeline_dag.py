from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='traffic_pipeline',
    default_args=default_args,
    description='Pipeline Smart City Traffic : traitement batch toutes les 60 secondes',
    schedule_interval=timedelta(seconds=60),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['smart-city', 'traffic', 'spark'],
) as dag:

    raw_to_processed = BashOperator(
        task_id='raw_to_processed',
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          /opt/spark-jobs/raw_to_processed.py
        """,
    )

    processed_to_analytics = BashOperator(
        task_id='processed_to_analytics',
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          /opt/spark-jobs/processed_to_analytics.py
        """,
    )

    analytics_to_postgres = BashOperator(
        task_id='analytics_to_postgres',
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --jars /opt/spark-jobs/jars/postgresql-42.7.1.jar \
          /opt/spark-jobs/analytics_to_postgres.py
        """,
    )

    # Ordre d'exécution : raw → processed → analytics → postgres
    raw_to_processed >> processed_to_analytics >> analytics_to_postgres