# Use the official Airflow image as the base
FROM apache/airflow:2.10.3

# Switch to root user to install system dependencies
USER root

# Install system dependencies, including Java and Kerberos
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    wget \
    curl \
    build-essential \
    python3-dev \
    krb5-config \
    libkrb5-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64


# Switch back to the airflow user
USER airflow

# Upgrade pip, setuptools, and wheel
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy requirements.txt and install dependencies
RUN pip install apache-airflow==2.10.3 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-hdfs \
    pyspark \
    pandas \
    requests \
    beautifulsoup4 \
    urllib3 \
    hdfs \
    psycopg2-binary


# Create necessary directories for Airflow
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Copy your project files, if necessary (adjust the source path as needed)
COPY . /opt/airflow/

# Set the default command for Airflow
CMD ["bash"]
