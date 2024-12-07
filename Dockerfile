# Use Ubuntu 22.04 LTS as base
FROM ubuntu:22.04

# Set environment variables
ENV AIRFLOW_HOME=/usr/local/airflow
ENV JUPYTER_CONFIG_DIR=/home/project/.jupyter

# Install necessary system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    python3-pip \
    python3-venv \
    postgresql-client \
    cron \
    vim \
    nano \
    sudo && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a symlink for Python 3 as Python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Add user 'project'
RUN useradd -ms /bin/bash project && \
    echo "project:jhu" | chpasswd && \
    usermod -aG sudo project # Add project user to sudo group

# Install JupyterLab and Airflow
RUN pip install jupyterlab==4.1.5 apache-airflow==2.8.4 apache-airflow[cncf.kubernetes] virtualenv connexion[swagger-ui] 

# Expose the ports for JupyterLab, Airflow, and Flask API
EXPOSE 8888 8080 5001

# Set up directories for Airflow, Flask API, and JupyterLab
RUN mkdir -p /usr/local/airflow/dags /home/project/api && \
    chown -R project:project /usr/local/airflow /home/project

# Copy project files
COPY airflow/dags/ /usr/local/airflow/dags/
COPY api/ /home/project/api/

# Create the logs directory in the container
RUN mkdir -p /home/project/logs

# Copy the logging setup file into the container
COPY logs/logging_setup.py /home/project/logs/logging_setup.py

# Set working directory
WORKDIR /home/project

# Copy the requirements.txt file into the container at /home/jhu
COPY requirements.txt .

# Install Python packages from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
