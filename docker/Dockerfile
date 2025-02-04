#Dockerfile
FROM apache/airflow:2.10.4-python3.9
# Install additional dependencies
USER root
COPY requirements.txt ./requirements.txt
USER airflow
RUN pip install --upgrade pip
# Set up additional Python dependencies
RUN pip install --no-cache-dir -r ./requirements.txt