import logging
from src.aws.athena_queries import (
    athena_wait_for_query_completion, 
    create_athena_client, 
    athena_start_query_execution, 
    athena_get_query_results, 
    transform_athena_results_to_dataframe
)
from src.aws.airflow_connection import get_aws_connection_info
from src.setup.settings import athena_output_queries

def execute_athena_query(query: str, output_location: str):
    """
        Executes an Athena query and retrieves the results.
        Args:
            query (str): The SQL query to execute.
            output_location (str): The S3 location for query results.
        Returns:
            list: The query results.
    """
    access_key, secret_key, region_name = get_aws_connection_info()
    athena_client = create_athena_client(region_name=region_name, access_key=access_key, secret_key=secret_key)
    query_execution_id = athena_start_query_execution(athena_client=athena_client, query=query, output_location=output_location)
    athena_wait_for_query_completion(athena_client=athena_client, query_execution_id=query_execution_id)
    results = athena_get_query_results(athena_client=athena_client, query_execution_id=query_execution_id)
    transformed_results = transform_athena_results_to_dataframe(results)
    return transformed_results

def main():
    """
        Main function to execute an Athena query and logging.info the results.
        This function defines a sample SQL query to retrieve data from the Open Brewery database in Athena. It then calls the execute_athena_query function to run the query and retrieves the results. Finally, it logging.infos the query results to the console.
    """
    query = "SELECT * FROM openbrewery_gold_db.openbrewery_aggregated_db;"
    output_location = athena_output_queries
    results = execute_athena_query(query=query, output_location=output_location)
    logging.info("Athena Query Results:")
    for row in results:
        logging.info(row)

if __name__ == "__main__":
    main()