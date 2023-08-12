from flask_restful import Api, Resource, reqparse
from repositories.reports import Reports as reports
class DepartmentHires(Resource):
    def get(self,year):
        rows = []
        result = reports.departments_by_hires(year)
        print(result)
        for row in result:
            rows.append(dict(row.items()))
        
        return rows