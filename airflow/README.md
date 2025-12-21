# Airflow Setup for Batch Processing

This directory contains the configuration for running Apache Airflow using Docker Compose.

## Structure

- `dags/`: Place your DAG files (Python scripts) here.
- `plugins/`: Custom plugins for Airflow.
- `config/`: Configuration files.
- `Dockerfile`: Custom Docker image definition (installs additional requirements).
- `requirements.txt`: Python packages to install in the Airflow image.

## How to Run

1.  Navigate to the root of the project (where `docker-compose.yml` is located).
2.  Build the Airflow image:
    ```bash
    docker-compose build
    ```
3.  Start the services:
    ```bash
    docker-compose up -d
    ```
4.  Access the Airflow UI at [http://localhost:8080](http://localhost:8080).
    - **Username**: `admin`
    - **Password**: `admin`

## Sample DAG

A sample DAG `sample_batch_processing` is provided in `dags/sample_batch_dag.py`. It demonstrates a simple flow with Bash and Python operators.

## Connecting to Spark

To submit Spark jobs from Airflow, you can use the `SparkSubmitOperator`. You will need to ensure the Airflow container can communicate with the Spark Master container (which is already set up in the `bigdata-network`).
