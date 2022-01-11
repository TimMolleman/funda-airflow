from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.contrib.operators.s3_delete_objects_operator import S3DeleteObjectsOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
from scripts import data_transfer
import pendulum


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 24, tzinfo=pendulum.timezone('Europe/Amsterdam')),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

s3_to_sql = DAG('s3_to_sql', 
                default_args=default_args, 
                schedule_interval='0 15 * * *',
                catchup=False)


write_links_from_s3_to_sql = PythonOperator(
    task_id='write_links_from_s3_to_sql',
    python_callable=data_transfer.write_s3_file_to_sql_table,
    dag=s3_to_sql,
    retries=1
)

s3_sensor_most_recent_data = S3KeySensor(
    task_id='s3_sensor_most_recent_data',
    bucket_key='s3://funda-airflow/house-links/filtered-links-most-recent/filtered_most_recent.csv',
    bucket_name=None,
    aws_conn_id='aws_s3_connection',
    dag=s3_to_sql,
    timeout=1200,
    retries=0
)

s3_delete_most_recent_data_file = S3DeleteObjectsOperator(
    task_id='s3_delete_most_recent_data_file',
    bucket='funda-airflow',
    keys='house-links/filtered-links-most-recent/filtered_most_recent.csv',
    aws_conn_id='aws_s3_connection',
    dag=s3_to_sql
)

trigger_model_training = TriggerDagRunOperator(
    task_id='trigger_model_training',
    trigger_dag_id='funda_train_model',
    dag=s3_to_sql
)

trigger_link_history_cleaner = TriggerDagRunOperator(
    task_id='trigger_link_history_cleaner',
    trigger_dag_id='link_history_cleaner',
    dag=s3_to_sql
)

s3_sensor_most_recent_data >> write_links_from_s3_to_sql >> s3_delete_most_recent_data_file >> [trigger_model_training, trigger_link_history_cleaner]
