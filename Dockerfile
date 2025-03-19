FROM apache/airflow:2.10.5

# Install Spark Provider
RUN pip install apache-airflow-providers-apache-spark