from flask import Flask, Blueprint
from flask_restful import Api
import os
import logging as logs
from resources.data_upload import DataUpload

app = Flask(__name__)


logs.basicConfig(level=logs.INFO)

bp = Blueprint('api', __name__)

api = Api(app)
api.init_app(bp)
app.register_blueprint(bp, url_prefix="/api")

api.add_resource(DataUpload, '/upload')

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8081)))