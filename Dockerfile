FROM apache/airflow:2.9.1

# Switch to airflow user (required by Airflow image)
USER airflow

# Copy dependency list
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt