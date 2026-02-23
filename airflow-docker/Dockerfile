FROM apache/airflow:3.1.7

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    yfinance \
    pandas \
    loguru \
    pyspark \
    google-cloud-bigquery \
    google-cloud-bigquery-storage \
    pyarrow \
    pandas-gbq \
    dbt-bigquery