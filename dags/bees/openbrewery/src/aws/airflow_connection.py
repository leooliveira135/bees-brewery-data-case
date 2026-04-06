import logging
from airflow.hooks.base import BaseHook

def get_aws_connection_info():
    logging.info("Getting data from the aws-airflow connection")
    conn = BaseHook.get_connection('aws-airflow')
    access_key = conn.login
    secret_key = conn.password
    region_name = conn.extra_dejson.get("region_name")
    logging.info("Data from aws-airflow collected successfully")
    return access_key, secret_key, region_name