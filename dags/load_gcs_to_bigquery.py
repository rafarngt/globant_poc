from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'rafael-noriega',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_gcs_to_bigquery',
    default_args=default_args,
    schedule_interval=None,  # Sin Schedule, Ejecucion Manual
)

project_id = 'poc-globant-data'
dataset_id = 'raw'
gcs_bucket = 'tables-backups-66b307/20230812' # asignar carpeta 

tables_and_files = {
    'departments': 'departments.avro',
    'hired_employees': 'hired_employees.avro',
    'jobs': 'jobs.avro',
}

load_tasks = []

for table, file_name in tables_and_files.items():
    load_task = GCSToBigQueryOperator(
        task_id=f'load_{table}_to_bigquery',
        bucket=gcs_bucket,
        source_objects=[file_name],
        destination_project_dataset_table=f'{project_id}.{dataset_id}.{table}',
        schema_fields=[],  # Especificar el esquema si es necesario
        autodetect=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',  
        source_format='AVRO',
        dag=dag,
    )
    load_tasks.append(load_task)

# Configurar las dependencias entre las tareas
for load_task in load_tasks:
    load_task
