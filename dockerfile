# Use the official Airflow image as the base
FROM apache/airflow:2.10.3

# Switch to root user to install system dependencies
USER root

# Install system dependencies, including Java and Kerberos
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    wget \
    curl \
    build-essential \
    python3-dev \
    krb5-config \
    libkrb5-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to the airflow user
USER airflow

# Upgrade pip, setuptools, and wheel
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy requirements.txt and install dependencies
RUN pip install apache-airflow==2.10.3 apache-airflow-providers-apache-spark==4.11.3 apache-airflow-providers-apache-hdfs==4.6.0 pyspark==3.5.3 pandas==2.2.3 requests==2.32.3 beautifulsoup4==4.12.3 urllib3==2.2.3 hdfs==2.7.3 psycopg2-binary==2.9.10

# Create necessary directories for Airflow
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Copy your project files, if necessary (adjust the source path as needed)
COPY . /opt/airflow/

# Set the default command for Airflow
CMD ["bash"]
