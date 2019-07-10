import json

from flask import request, Response
from flask_restful import Resource
# for second-level logging
import logging
# for handling elasticsearch
from elasticsearch import Elasticsearch

from memex_logging.models.log import Log
from memex_logging.utils.utils import Utils


class LoggingResourceBuilder(object):
    """
    Logic class used to create enable the endpoints. This class is used in ws.py
    """
    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (LogGeneralLog, '/log', (es,)),
            (LogGeneralLogs, '/logs', (es,))
        ]


class LogGeneralLog(Resource):
    """
    This class can be used to log a single log message into the elasticsearch instance
    """

    def __init__(self, es: Elasticsearch):
        """
        Function to gather the parameters that are going to be used in the various methods
        :param es: the elastic search instance
        """
        self._es = es

    # TODO get log


class LogGeneralLogs(Resource):
    """
    This class can be used to log an array of log messages.
    """

    def __init__(self, es: Elasticsearch):
        """
        Function to gather the external parameters that are going to be used in the other methods
        :param es: the elasticsearch instance
        """
        self._es = es

    def post(self) -> Response:
        """
        Add a batch of log messages to the database. The logs must be passed in the request body.
        This method log the messages with index <project-name>-logging-<yyyy>-<mm>-<dd>
        :return: the HTTP response
        """
        logging.warning("LOGGING.API Starting to log a new set of messages")

        logs_received = request.json
        log_ids = []

        for log in logs_received:
            # push the message in the database
            utils = Utils()

            try:
                temp_log = Log.from_rep(log)
                project_name = utils.extract_project_name(log)
                date = utils.extract_date(log)
                index_name = project_name + "-logging-" + date
                query = self._es.index(index=index_name, doc_type='_doc', body=temp_log.to_repr())
                log_ids.append(query['_id'])
            except:
                logging.error("LOGGING.API Failed to log")
                logging.error(log)

        json_response = {
            "logId": log_ids,
            "status": "ok",
            "code": 200
        }

        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200

        return resp
