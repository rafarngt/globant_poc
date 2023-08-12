from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'rafael-noriega',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'backup_bigquery_to_gcs',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)
project_id = Variable.get("project_id")
dataset_id = Variable.get("dataset")
gcs_bucket = 'gs://tables-backups-66b307'

# Obtener la lista de tablas del conjunto de datos
tables = ['jobs', 'hired_employees', 'departments']

# Tareas para respaldar las tablas de BigQuery a GCS en formato AVRO
backup_tasks = []

for table in tables:
    backup_task = BigQueryToGCSOperator(
        task_id=f'backup_{table}_to_gcs',
        gcp_conn_id="google_cloud_default",
        project_id=project_id,
        source_project_dataset_table=f"{project_id}.{dataset_id}.{table}",
        destination_cloud_storage_uris=[f"{gcs_bucket}/{{{{ ds_nodash }}}}/{table}.avro"],
        export_format='AVRO',
        field_delimiter=',',
        dag=dag,
    )
    backup_tasks.append(backup_task)

# Configurar las dependencias entre las tareas
for backup_task in backup_tasks:
    backup_task
