This repository is a challenge for the Data Engineer role, which uses the following technologies and services: 

- [x] GCP cloud
    - GCS (Google Cloud Storage)
    - Cloud Run
    - Bigquery
    - Composer (optional) 
- [x] Python 3.x
- [x] Flask_RESTful API
- [x] Pandas

### Project directory structure

>```env
>|- api/ Application module (API REST)
>| |- repositories/          folder that contains classes in charge of querying the BQ
>| |- |.. reports.py             file contains the Report class, responsible for having the functions that retrieve data for the reporting endpoints
>| |- resources/             folder containing the classes defined in each of the endpoints
>| |- |.. data_upload.py            definition of the endpoints for /api/upload (bulk file upload)    
>| |- |.. department_hires.py        definition of the endpoints for /api/reports/hires_by_quarter/{year}
>| |- |.. hires_by_quarter.py          definition of the endpoints for /api/reports/hires_by_quarter/{year}
>| |- utils/                 folder contains utility functions 
>| |- |.. utils.py           
>| |- app.py           main project file
>| |- requirements.txt         Libraries to install 
>|- dags/  contains dag for migration, backup and restore tables
>|- dags/backups_bigquery_to_gcs.py  DAG for create backups of BQ tables
>|- dags/data_migration_dag.py  DAG for create migration of initial tablas in BQ
>|- dags/load_gcs_to_bigquery.py DAG to restore Backup in BQ
>|- data/csv/.. .csv files
>|- data/ddl/.. .sql ddl files
>|- data/dml/.. .sql dml files
>|- data/schema/.. json schemas for BQ
>|- doc/.. Documentions
>|.. .gitignore
>|.. README.md
>```
# Architeceture: 

# Pre requeriments: 
- [X] Have Docker and Docker Compose Installed
- [X] Have Airflow with docker or Composer Instance
- [X] Have a GCP Account
- [X] for GCP:
    - Create a Service Account with these roles: 
        - BigQuery Admin
        - Composer Administrator
        - Storage Admin
        - Storage Object Admin
    - Generate service account JSON key

# QuickStart

### Local Usage
Create a virtual environment by running:

>```shell
>python -m venv .venv
>
>```

The virtual environment should be activated every time you start a new shell session before running subsequent commands:

> On Linux/MacOS:
> ```shell
> source .venv/bin/activate
> ```

> On Windows:
> ```shell
> .venv\Scripts\activate.bat
> ```

set the GOOGLE_APPLICATION_CREDENTIALS variable
> ```
> export GOOGLE_APPLICATION_CREDENTIALS=/path/with/json_key.json
> ```
### Configure Airflow

To deploy Airflow on Docker Compose, you should fetch docker-compose.yaml.
> ```shell
>curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.3/docker-compose.yaml'
> ```

create folders base
> ```shell
> mkdir -p ./dags ./logs ./plugins ./config
> echo -e "AIRFLOW_UID=$(id -u)" > .env
> ```

Initialize the database

> ```shell
> docker compose up airflow-init
> ```

go to the url http://127.0.0.1:8080/home in your browser
login and password : airflow

for more informacion: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

Configure Connection: 
You need a connection to the BQ and GCP Storage services, so go to the `Admin->Connection` path and create a connection and type `GCP providers`, there load the `json key` created previously. 

Configure Variable: 
you also need to load the variables used by the different dags, so go to `Admin->Variables` and load the `variable.json` file found in the `dags` folder.

### Run Dags

to run the dags, you must load them into the dags folder previously created, after launching the airflow service, then wait a few minutes for the dags to appear in the list. 

### Run API

locate us inside the api folder: 
> ```
> cd api
> ```

install dependencies.
> ```
> pip install -r requitements.txt
> ```

run locally
> ```
> python app.py 
> ```

curls to test
upload file endpoint: 
> ```
> curl --location 'http://127.0.0.1:8081/api/upload' \
> --form 'file=@"/path/with/file/hired_employees.csv"'
> ```

hires_by_quarter endpoint: 
> ```
> curl --location 'http://127.0.0.1:8081/api/reports/hires_by_quarter/2021'
> ```

department_hires endpoint: 
> ```
> curl --location 'http://127.0.0.1:8081/api/reports/department_hires/2021'
> ```


### GCP Usage
Cloud Storage (GCS)
BiQuery
Cloud RUN






gcloud builds submit --tag gcr.io/poc-globant-data/flask-api

 gcloud run services update flask-api --update-env-vars PROJECT_ID="poc-globant-data",BQ_DATASET="raw,GCS_BUCKET=invalid_records-90c6f489"