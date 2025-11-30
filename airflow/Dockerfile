FROM apache/airflow:3.1.2-python3.10

# Switch back to airflow user
USER airflow

# Copy Python dependencies and install
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt