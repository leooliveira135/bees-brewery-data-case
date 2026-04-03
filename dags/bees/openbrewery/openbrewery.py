from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator

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
    schedule_interval='@daily',
    start_date='2026-04-01',
    catchup=False,
    tags=['bees', 'openbrewery']
) as dag:
    extract_task = PythonVirtualenvOperator(
        task_id='extract data from API',
        python_callable=extract,
        requirements="requirements.txt",
        on_success_callback=lambda context: print("Extract task completed successfully!"),
        on_failure_callback=lambda context: print("Extract task failed!"),
    )
    extract_task.doc_md = """
    ### Extract Task
    This task is responsible for extracting data from the Open Brewery DB API and loading it into a staging area. It uses the `fetch_data` function from the `src.etl.fetch_data` module to perform the extraction. The task is scheduled to run daily and will print a success or failure message upon completion.
    """

    transform_task = PythonVirtualenvOperator(
        task_id='transform data and load to data warehouse',
        python_callable=transform,
        on_success_callback=lambda context: print("Transform task completed successfully!"),
        on_failure_callback=lambda context: print("Transform task failed!")
    )
    transform_task.doc_md = """
    ### Transform Task
    This task is responsible for transforming the extracted data and loading it into the data warehouse. It uses the `transform_data` function from the `src.etl.transformation_data` module to perform the transformation. The task is scheduled to run daily and will print a success or failure message upon completion.
    """

    load_task = PythonVirtualenvOperator(
        task_id='load data to data warehouse and aggregate',
        python_callable=load,
        on_success_callback=lambda context: print("Load task completed successfully!"),
        on_failure_callback=lambda context: print("Load task failed!")
    )
    load_task.doc_md = """
    ### Load Task
    This task is responsible for loading the transformed data into the data warehouse and performing aggregation. It uses the `load_data` function from the `src.etl.aggregation_data` module to perform the loading and aggregation. The task is scheduled to run daily and will print a success or failure message upon completion.
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

    Tags: `bees`, `openbrewery`
    """

    extract_task >> transform_task >> load_task