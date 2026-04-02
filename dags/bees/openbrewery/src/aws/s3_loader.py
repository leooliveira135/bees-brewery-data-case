import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_s3(file_data, bucket_name, object_name):
    """
        Uploads a file to an S3 bucket. The function takes the file data, bucket name, and object name as parameters, and uses the S3Hook from Airflow's Amazon provider to handle the upload process. The file data is converted to a JSON string before being uploaded to ensure it is stored in a structured format in the S3 bucket. This function is essential for saving the transformed data back to S3 for further use in the ETL pipeline or for analysis.
        Args:
            file_data (dict): The data to be uploaded to S3, typically in the form of a dictionary that will be converted to JSON.
            bucket_name (str): The name of the S3 bucket where the file will be uploaded.
            object_name (str): The key or name of the object in the S3 bucket under which the file will be stored.
        returns:
            None: This function does not return any value. It performs the action of uploading the file to S3 and prints a confirmation message upon successful upload.
    """
    s3_hook = S3Hook(aws_conn_id='aws-airflow')
    s3_hook.load_string(
        string_data=json.dumps(file_data),
        bucket_name=bucket_name,
        key=object_name
    )
    print(f"File uploaded to s3://{bucket_name}/{object_name}")
