# Jupyter Notebook Configuration for OLake Spark Demo
# This file configures Jupyter to work optimally with Spark and Iceberg

import os

# Server Configuration
c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = 8888
c.ServerApp.open_browser = False
c.ServerApp.token = 'olake123'
c.ServerApp.password = ''
c.ServerApp.allow_root = True
c.ServerApp.allow_remote_access = True

# Notebook Directory
c.ServerApp.root_dir = '/home/jovyan/work'
c.ServerApp.notebook_dir = '/home/jovyan/work'

# Enable JupyterLab by default
c.ServerApp.default_url = '/lab'

# Kernel Configuration
c.KernelManager.autorestart = True

# Terminal Configuration
c.ServerApp.terminals_enabled = True

# File Management
c.ContentsManager.allow_hidden = False
c.FileContentsManager.delete_to_trash = False

# Security
c.ServerApp.disable_check_xsrf = False

# Logging
c.Application.log_level = 'INFO'

# Spark Environment Variables (passed to kernel)
c.Spawner.environment = {
    'SPARK_HOME': '/usr/local/spark',
    'HADOOP_CONF_DIR': '/opt/spark/conf',
    'SPARK_CONF_DIR': '/opt/spark/conf',
    'PYSPARK_PYTHON': '/opt/conda/bin/python',
    'PYSPARK_DRIVER_PYTHON': '/opt/conda/bin/python',
    'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID', 'minio'),
    'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY', 'minio123'),
    'AWS_REGION': os.environ.get('AWS_REGION', 'us-east-1'),
}

# Performance Settings
c.ServerApp.iopub_data_rate_limit = 10000000
c.ServerApp.iopub_msg_rate_limit = 10000

# Session Settings
c.ServerApp.shutdown_no_activity_timeout = 0
c.MappingKernelManager.cull_idle_timeout = 0
c.MappingKernelManager.cull_interval = 0

print("✓ Jupyter Notebook configured for Spark/Iceberg")
