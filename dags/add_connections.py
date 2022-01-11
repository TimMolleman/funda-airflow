from airflow import DAG
from airflow.utils import db
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection

from datetime import datetime, timedelta
import os
import json
import pendulum


def _add_connection(ds, **kwargs):
    """Add a connections"""
    new_conn = Connection(
                        conn_id=kwargs['conn_id'],
                        conn_type=kwargs['conn_type'],
                        login=kwargs['login'],
                        password=kwargs['password'],
                        host=kwargs['host'],
                        port=kwargs['port'],
                        extra=kwargs['extra']
    )

    with db.create_session() as session:
        
        if not (session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first()):
            session.add(new_conn)
            session.commit()
            print(f'Added connection {new_conn.conn_id}')
        else:
            msg = 'A connection with `conn_id`={conn_id} already exists. Updating the connection'
            msg = msg.format(conn_id=new_conn.conn_id)
            print(msg)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 24, tzinfo=pendulum.timezone('Europe/Amsterdam')),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        'add_connections',
        default_args=default_args,
        schedule_interval='@once',
    )

add_aws_connection = PythonOperator(
    dag=dag,
    task_id='add_aws_connection',
    python_callable=_add_connection,
    provide_context=True,
    op_kwargs={'conn_id': 'aws_conn_id',
               'conn_type': 'amazon web services',
               'login': os.environ['AMAZON_ACCESS_KEY_ID'],
               'password': os.environ['AMAZON_SECRET_ACCESS_KEY'],
               'host': None,
               'port': None,
               'extra': json.dumps({'region_name': 'eu-west-2'})}
)

add_sql_connection = PythonOperator(
    dag=dag,
    task_id='add_sql_connection',
    python_callable=_add_connection,
    provide_context=True,
    op_kwargs={'conn_id': 'sql_conn_id',
               'conn_type': 'mysql',
               'login': os.environ['FUNDA_DB_USER'],
               'password': os.environ['FUNDA_DB_PW'],
               'host': os.environ['FUNDA_DB_HOST'],
               'port': '3306',
               'extra': None
               }
)

add_aws_s3_connection = PythonOperator(
    dag=dag,
    task_id='add_aws_s3_connection',
    python_callable=_add_connection,
    provide_context=True,
    op_kwargs={
        'conn_id': 'aws_s3_connection',
        'conn_type': 's3',
        'login': None,
        'password': None,
        'host': None,
        'port': None,
        'extra': json.dumps({'aws_access_key_id': os.environ['AMAZON_ACCESS_KEY_ID'],
                             'aws_secret_access_key': os.environ['AMAZON_SECRET_ACCESS_KEY']})
    }
)
