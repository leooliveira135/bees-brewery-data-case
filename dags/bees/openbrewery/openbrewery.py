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
    catchup=False
) as dag:
    extract_task = PythonVirtualenvOperator(
        task_id='extract',
        python_callable=extract,
        requirements="requirements.txt"
    )

    transform_task = PythonVirtualenvOperator(
        task_id='transform',
        python_callable=transform
    )

    load_task = PythonVirtualenvOperator(
        task_id='load',
        python_callable=load
    )

    extract_task >> transform_task >> load_task