FROM jupyter/scipy-notebook:latest

# Install additional Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set a custom working directory
WORKDIR /home/jovyan/work