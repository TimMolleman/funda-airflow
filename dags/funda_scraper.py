from airflow import DAG
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import io
import pendulum


def _trigger_lambda_link_scraper():
    lambda_ = AwsLambdaHook(
        aws_conn_id='aws_conn_id',
        function_name='funda-link-scraper'
    )

    lambda_.invoke_lambda(payload=io.BytesIO())


def _trigger_lambda_link_cleaner():
    lambda_ = AwsLambdaHook(
        aws_conn_id='aws_conn_id',
        function_name='funda-link-cleaner'
    )

    lambda_.invoke_lambda(payload=io.BytesIO())


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 24, tzinfo=pendulum.timezone('Europe/Amsterdam')),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

funda_scraper = DAG('funda_scraper', 
                    default_args=default_args, 
                    schedule_interval='0 15 * * *',
                    catchup=False)


trigger_lambda_link_scraper = PythonOperator(
    task_id='trigger_lambda_link_scraper',
    python_callable=_trigger_lambda_link_scraper,
    dag=funda_scraper,
    retries=1
)

trigger_lambda_link_cleaner = PythonOperator(
    task_id='trigger_lambda_link_cleaner',
    python_callable=_trigger_lambda_link_cleaner,
    dag=funda_scraper,
    retries=1
)

trigger_lambda_link_scraper >> trigger_lambda_link_cleaner
