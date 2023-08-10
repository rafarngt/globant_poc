from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
import ast 

def on_failure_callback(**kwargs):
    # This function will be executed when the task fails
    print("Task failed. Adding additional step for logging.")

default_args = {
    'owner': 'rafael_noriega',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'on_failure_callback': on_failure_callback,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'data_migration_dag',
    default_args=default_args,
    description='DAG to migrate data to BigQuery',
    schedule_interval=timedelta(days=1),
)

folders = ast.literal_eval(Variable.get("folders"))  # get a list of the folders to be migrated 
raw_bucket_name =Variable.get("raw_bucket_name")
data_folder = 'gs://{}/data/'.format(raw_bucket_name)  # Folder for data files in GCS

# Define your tasks here...
task1 = DummyOperator(
    task_id='migration_start',
    dag=dag,
)

# Define your PythonOperator for additional step on failure
additional_step_on_failure = PythonOperator(
    task_id=f'on_failure_',
    python_callable=on_failure_callback,  # Call the callback function
    provide_context=True,  # Pass the context to the callback function
    trigger_rule='one_failed',
    dag=dag,
)

for folder_name in folders:
    
    base_path = "year={{ execution_date.year }}/mount={{ execution_date.month }}/day={{ execution_date.day }}"  # Folder for schema files in GCS

    schema_file_path = f"{base_path}/{folder_name}/{folder_name}.json"
    data_file_path = f"{base_path}/{folder_name}/{folder_name}.csv"
    project_id = Variable.get("project_id")
    bq_dataset = Variable.get("dataset")



    load_task = GCSToBigQueryOperator(
        task_id=f'load_{folder_name}_to_bigquery',
        schema_object=schema_file_path,  # Read schema from file in GCS
        destination_project_dataset_table=f'{project_id}.{bq_dataset}.{folder_name}',  # Replace with your project and dataset
        bucket=raw_bucket_name,
        source_objects=[data_file_path],  # 
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        field_delimiter=',',
        gcp_conn_id='google_cloud_default',
        autodetect=False,
        dag=dag
    )
    
  
    task1 >> load_task  >> additional_step_on_failure