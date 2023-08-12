from flask_restful import Api, Resource, reqparse
from repositories.reports import Reports as reports
class HiresByQuarter(Resource):
    def get(self,year):
        rows = []
        result = reports.hiries_by_quarter(year)
        for row in result:
            rows.append(dict(row.items()))

        return rows
