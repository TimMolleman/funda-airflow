import pandas as pd
import sqlalchemy

from airflow.hooks.S3_hook import S3Hook

from scripts import connectors


def write_s3_file_to_sql_table():
    """Write s3 most recent housing data to sql table."""
    # Create s3 connection
    s3 = S3Hook('aws_s3_connection').get_conn()
    file = s3.get_object(Bucket='funda-airflow', Key='house-links/filtered-links-most-recent/filtered_most_recent.csv')

    # Get data from the object and get links
    data_df = pd.read_csv(file['Body'])
    links = data_df['link']
    
    # Get sql connection and transaction
    connection, transaction = connectors.connect_to_db(database_name='funda_project_db')

    try:
        # First delete links already present in city_info table as to avoid duplicates
        sql = sqlalchemy.text(f"""
                            DELETE FROM city_info AS ci 
                            WHERE ci.link IN :links 
                            """)
        sql = sql.bindparams(links=tuple(links,))
        connection.execute(sql)

        # Then, write the records in data_df to the city_info table
        data_df.to_sql('city_info', con=connection, if_exists='append', index=False)
        transaction.commit()

        print('Finished uploading new city info')

    except Exception as e:
        transaction.rollback()
        raise e

