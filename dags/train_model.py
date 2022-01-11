from airflow import DAG
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import io
import pendulum


def _trigger_lambda_train_model():
    lambda_ = AwsLambdaHook(
        aws_conn_id='aws_conn_id',
        function_name='funda-model-trainer'
    )

    lambda_.invoke_lambda(payload=io.BytesIO())


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 24, tzinfo=pendulum.timezone('Europe/Amsterdam')),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)}

funda_train_model = DAG('funda_train_model', 
                        default_args=default_args, 
                        schedule_interval=None,
                        catchup=False)


trigger_lambda_train_model = PythonOperator(
    task_id='trigger_lambda_train_model',
    python_callable=_trigger_lambda_train_model,
    dag=funda_train_model,
    retries=1
)
