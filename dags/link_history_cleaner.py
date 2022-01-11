from airflow import DAG
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import io
import pendulum


def _trigger_lambda_link_history_cleaner():
    lambda_ = AwsLambdaHook(
        aws_conn_id='aws_conn_id',
        function_name='funda-history-link-cleaner'
    )

    lambda_.invoke_lambda(payload=io.BytesIO())


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 24, tzinfo=pendulum.timezone('Europe/Amsterdam')),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

link_history_cleaner = DAG('link_history_cleaner', 
                           default_args=default_args,
                           schedule_interval=None)


trigger_lambda_history_cleaner = PythonOperator(
    task_id='trigger_lambda_history_cleaner',
    python_callable=_trigger_lambda_link_history_cleaner,
    dag=link_history_cleaner,
    retries=1
)
