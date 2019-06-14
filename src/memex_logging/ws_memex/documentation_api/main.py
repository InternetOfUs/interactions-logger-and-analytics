from flask import Response
from flask_restful import Resource

import os


class DocumentationResourceBuilder(object):
    """
    Logic class used to create enable the endpoints. This class is used in ws.py
    """
    @staticmethod
    def routes():
        return [
            (Documentation, '/documentation', ())
        ]


class Documentation(Resource):
    """
    This class can be used to log a single log message into the elasticsearch instance
    """

    def get(self) -> Response:
        """
        Get the documentation in a raw format
        :return: the documentation in a raw format
        """
        text_response = ""

        f = open("%s/openapi.yaml" % os.getenv("DOCUMENTATION_PATH", "../../../documentation"), "r")
        for line in f:
            text_response += line + "\n"
        f.close()

        resp = Response(text_response, mimetype='text/plain', headers={"Access-Control-Allow-Origin": "*"})
        resp.status_code = 200
        return resp
