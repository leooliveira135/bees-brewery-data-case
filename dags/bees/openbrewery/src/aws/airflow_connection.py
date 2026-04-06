from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def get_aws_connection_info():
    print("Getting data from the aws-airflow connection")
    conn = BaseHook.get_connection('aws-airflow')
    access_key = conn.login
    secret_key = conn.password
    region_name = conn.to_dict()['extra']['region']
    print("Data from aws-airflow collected successfully")
    return access_key, secret_key, region_name