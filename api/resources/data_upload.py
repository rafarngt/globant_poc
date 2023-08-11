import logging as logs
from flask_restful import Resource, reqparse
from google.cloud import bigquery
from google.cloud import storage
import werkzeug
from werkzeug.utils import secure_filename
from utils.utils_funtions import UtilsFuntions as utilsFuntions
import os


class DataUpload(Resource):
    """API resource for uploading data to BigQuery."""
    section = 'upload-data'
    # Initialize BigQuery client
    client = bigquery.Client()
    

    def post(self):
        """Handles the POST request for uploading data."""
        logs.info("endpoint {}".format(self.section))
        parser = reqparse.RequestParser()
        parser.add_argument('file', type=werkzeug.datastructures.FileStorage, location='files', required=True)
        args = parser.parse_args()
        file = args['file']

        if not file:
            return {'message': 'No se encontró ningún archivo'}, 400

        filename = secure_filename(file.filename)
        table_name, extension = os.path.splitext(filename)

        try:
            if not utilsFuntions.validate_table_name(table_name):
                return {'message': f'El nombre de archivo {table_name} no es válido'}, 400

            if extension != '.csv':
                return {'message': 'Formato de archivo no compatible'}, 400
            
            table_id = f'{os.environ["PROJECT_ID"]}.{os.environ["BQ_DATASET"]}.{table_name}'
            table = self.client.get_table(table_id)
            schema = table.schema
            df = utilsFuntions.read_csv_into_dataframe(file, schema)

            df_cleaned, df_removed = utilsFuntions.remove_rows_with_nan(df)

            errors = self.client.insert_rows_json(table_id, df_cleaned.to_dict(orient='records'))

            gcs_client = storage.Client()
            gcs_bucket = os.environ["GCS_BUCKET"]
            if errors:
                utilsFuntions.save_failed_rows_to_gcs(errors, filename, gcs_client,gcs_bucket )
                
            if len(df_removed) > 0:
                utilsFuntions.save_removed_rows_to_gcs(df_removed, filename, gcs_client,gcs_bucket)

            return {'message': f'Registros insertados en {table_name}'}
        
        except Exception as e:
            return {'message': f'Error al procesar el archivo: {str(e)}'}, 500
