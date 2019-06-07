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

    def get(self) -> tuple:
        """
        Get the documentation in a raw format
        :return: the documentation in a raw format
        """
        text_response = ""
        print(os.getcwd())
        f = open("../../../documentation/openapi.yaml", "r")
        for line in f:
            text_response += line + "\n"
        f.close()
        return text_response, 200
