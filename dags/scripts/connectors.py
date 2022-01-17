from airflow.hooks.mysql_hook import MySqlHook
import sqlalchemy 


def connect_to_db(database_name: str):
    """Creates connection to database with sql_conn_id."""
    # Create sql connection string
    mysql_connection_uri = MySqlHook('sql_conn_id').get_uri()
    mysql_connection_uri = mysql_connection_uri + database_name

    # Create engine
    engine_app = sqlalchemy.create_engine(mysql_connection_uri, echo=False)

    # Start up connection and transaction
    connection = engine_app.connect()
    transaction = connection.begin()

    return connection, transaction
