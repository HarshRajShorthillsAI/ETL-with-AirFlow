# Use the official Apache Airflow image with the appropriate Python version
FROM apache/airflow:2.6.1-python3.9

# Switch to root to install Java and other packages
USER root
 
# Update package list and install OpenJDK 11
RUN apt-get update && apt-get install -y openjdk-11-jdk && apt-get clean

# Set JAVA_HOME environment variable (adjust if necessary)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch to the airflow user to install packages as recommended
USER airflow

# Install PySpark using pip as the airflow user
RUN pip install pyspark

RUN pip install apache-airflow

# Install Spark provider (optional: pin version for compatibility)
ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.9
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-celery>=3.3.0 \
    --constraint "${CONSTRAINT_URL}"
