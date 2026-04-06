import os
import requests
from src.setup.settings import endpoint, s3_bronze_bucket, aws_glue_role
from src.aws.s3_loader import upload_to_s3
from src.aws.glue_catalog import create_glue_database, create_glue_crawler, start_glue_crawler, list_glue_db_tables
from src.aws.airflow_connection import get_aws_connection_info

def fetch_data(endpoint):
    """
        Fetches brewery data from the Open Brewery DB API.
        The API is paginated, so we loop through pages until we get an empty response.
        Each page contains up to 200 breweries, and we continue fetching until we have all the data.
        Args:
            endpoint (str): The API endpoint with pagination parameters.
        Returns:
            list: A list of brewery data fetched from the API.
    """
    try:
        response = requests.get(endpoint)
        output = []
        for i in range(1, 1000):  # Arbitrary large number to ensure we fetch all pages
            print(f"Fetching page {i}...")
            response = requests.get(endpoint.format(page=i))
            output.extend(response.json())
        if response.status_code == 200:
            return output
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")
    except Exception as e:
        print(f"An error occurred while fetching data: {e}")
        return []
    
def create_glue_bronze_catalog():
    """
        Creates a Glue Catalog table for the Open Brewery data.
        This function defines the schema for the brewery data and creates a Glue Catalog table in the specified database. The table is created with the appropriate columns and data types to store the brewery information effectively. This allows for easy querying and analysis of the brewery data using AWS Glue and Athena.
    """    
    _, _, region_name = get_aws_connection_info()
    create_glue_database(database_name="openbrewery_db", aws_region=region_name)
    create_glue_crawler(
        crawler_name="openbrewery_crawler",
        role_arn=aws_glue_role,
        database_name="openbrewery_db",
        s3_path=f"s3a://{s3_bronze_bucket}/openbrewery_data.json",
        aws_region=region_name
    )
    start_glue_crawler(crawler_name="openbrewery_crawler", aws_region=region_name)
    tables = list_glue_db_tables(database_name="openbrewery_db", aws_region=region_name)
    print(f"Tables in Glue database 'openbrewery_db': {tables}")
    
def main():
    """
        Main function to fetch brewery data and upload it to S3.
        This function orchestrates the data fetching and uploading process. It first calls the fetch_data function to retrieve all brewery data from the Open Brewery DB API. Once the data is fetched, it prints the total number of breweries retrieved and then uploads the data to an S3 bucket using the upload_to_s3 function.
        Fetching data from the API is done in a paginated manner, ensuring that we retrieve all available brewery records. After fetching, the data is uploaded to the specified S3 bucket in JSON format.
    """
    data = fetch_data(endpoint)
    print(f"Total breweries fetched: {len(data)}")
    upload_to_s3(data, bucket_name=s3_bronze_bucket, object_name='openbrewery_data.json')
    create_glue_bronze_catalog()    

if __name__ == "__main__":
    main()