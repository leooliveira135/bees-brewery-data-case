from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime

def extract():
    """
        Extract data from the Open Brewery DB API and load it into a staging area.
    """
    from src.etl.fetch_data import main as fetch_data

    fetch_data()

def transform():
    """
        Transform the extracted data and load it into the data warehouse.
    """
    from src.etl.transformation_data import main as transform_data

    transform_data()

def load():
    """
        Load the transformed data into the data warehouse.
    """
    from src.etl.aggregation_data import main as load_data

    load_data()

with DAG(
    dag_id='openbrewery',
    schedule='@daily',
    start_date = datetime(2026, 4, 1),
    catchup=False,
    tags=['api', 'bees', 'openbrewery']
) as dag:
    extract_task = PythonVirtualenvOperator(
        task_id='extract_data_from_API',
        python_callable=extract,
    )
    extract_task.doc_md = """
    ### Extract Task
    This task is responsible for extracting data from the Open Brewery DB API and loading it into a staging area. It uses the `fetch_data` function from the `src.etl.fetch_data` module to perform the extraction.
    """

    transform_task = PythonVirtualenvOperator(
        task_id='transform_data_and_load_to_data_warehouse',
        python_callable=transform,
    )
    transform_task.doc_md = """
    ### Transform Task
    This task is responsible for transforming the extracted data and loading it into the data warehouse. It uses the `transform_data` function from the `src.etl.transformation_data` module to perform the transformation.
    """

    load_task = PythonVirtualenvOperator(
        task_id='load_data_to_data_warehouse_and_aggregate',
        python_callable=load,
        on_success_callback=lambda context: print("Load task completed successfully!"),
    )
    load_task.doc_md = """
    ### Load Task
    This task is responsible for loading the transformed data into the data warehouse and performing aggregation. It uses the `load_data` function from the `src.etl.aggregation_data` module to perform the loading and aggregation.
    """

    dag.doc_md = """
    # Open Brewery ETL DAG

    This DAG orchestrates a daily extract-transform-load (ETL) workflow for Open Brewery data.

    - **extract task**: pulls JSON data from Open Brewery DB API and saves into staging.
    - **transform task**: cleans, validates, and reshapes staging data for analytics.
    - **load task**: loads transformed data into the data warehouse and performs aggregation.

    Execution settings:
    - schedule_interval: `@daily`
    - start_date: `2026-04-01`
    - catchup: `False`

    Tags: `api`, `bees`, `openbrewery`
    """

    extract_task >> transform_task >> load_task