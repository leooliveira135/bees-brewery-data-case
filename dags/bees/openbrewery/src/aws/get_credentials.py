import json
from airflow.hooks.base import BaseHook

def get_aws_connection_info():
    """
        Retrieves AWS connection information from Airflow's connection management system.
        This function uses Airflow's BaseHook to access the connection details for a connection named 'aws-airflow'. It extracts the access key, secret key, and region name from the connection configuration and returns them as a tuple.
        Returns:
            tuple: A tuple containing the AWS access key, secret key, and region name.
    """
    print("Getting data from the aws-airflow connection")
    conn = BaseHook.get_connection('aws-airflow')
    extra = json.loads(conn.extra) if conn.extra else {}
    access_key = extra.get('access_key')
    secret_key = extra.get('secret_key')
    region_name = extra.get('region_name')
    print("Data from aws-airflow collected successfully")
    return access_key, secret_key, region_name