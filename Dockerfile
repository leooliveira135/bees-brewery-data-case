FROM apache/airflow:3.0.6

COPY dags/bees/openbrewery/requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt

ENV PYTHONPATH="/opt/airflow/dags"