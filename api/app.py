from flask import Flask
from flask_restful import Api, Resource, reqparse
import os
import json
import logging as logs
import werkzeug
import pandas as pd
from werkzeug.utils import secure_filename
from google.cloud import bigquery
from google.cloud import storage

app = Flask(__name__)
api = Api(app)
#TODO: add as environment
app.config['BQ_DATASET'] = 'raw'
app.config['PROJECT_ID'] = 'poc-globant-data'
app.config['GCS_BUCKET'] = 'invalid_records-90c6f489'
# Initialize BigQuery client
client = bigquery.Client()



class DataUpload(Resource):
    section = 'upload-data'

    def post(self):
        logs.info("endpoint {}".format(self.section))
        parser = reqparse.RequestParser()
        parser.add_argument('file', type=werkzeug.datastructures.FileStorage, location='files', required=True)
        args = parser.parse_args()
        file = args['file']

        if file:
            filename = secure_filename(file.filename)
            table_name, extension = os.path.splitext(filename)
       
            try:
        
                    if table_name not in ['departments', 'hired_employees', 'jobs']:
                        return {'message': f'El nombre de archivo {table_name} no es válido'}, 400

                    if extension == '.csv':
                        table_id = f'{app.config["PROJECT_ID"]}.{app.config["BQ_DATASET"]}.{table_name}'
                        table = client.get_table(table_id)
                        schema = table.schema
                        df = pd.read_csv(file, sep=',', header=None, names=[field.name for field in schema] )

                        # Inserta los registros en la tabla existente en BigQuery
                        errors = client.insert_rows_json(table_id, df.to_dict(orient='records'))

                        if errors:
                            gcs_client = storage.Client()
                            bucket = gcs_client.get_bucket(app.config['GCS_BUCKET'])
                            blob = bucket.blob(f'failed_rows/{filename}')
                            blob.upload_from_string(json.dumps(errors))

                        return {'message': f'Registros insertados en {table_name}'}
                    
                    else:
                        return {'message': 'Formato de archivo no compatible'}, 400
                   
                    
                    

            except Exception as e:
                    return {'message': f'Error al procesar el archivo: {str(e)}'}, 500


api.add_resource(DataUpload, '/upload')

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8081)))