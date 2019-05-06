"""
This module contains all the end-points for the logging APIs
"""

from flask import Flask, request
from flask_restful import Resource, Api, abort
# for second-level logging
import logging
from datetime import datetime
# for handling elasticsearch
from elasticsearch import Elasticsearch

from api.utils.utils import Utils


class LoggingResourceBuilder(object):
    """
    Logic class used to create enable the endpoints. This class is used in ws.py
    """
    @staticmethod
    def routes(es: Elasticsearch, project_name: str):
        return [
            (LogGeneralLog, '/log', (es, project_name,)),
            (LogGeneralLogs, '/logs', (es, project_name,))
        ]


class LogGeneralLog(Resource):
    """
    This class can be used to log a single log message into the elasticsearch instance
    """

    def __init__(self, es, project_name):
        """
        Function to gather the parameters that are going to be used in the various methods
        :param es: the elastic search instance
        :param project_name: the name of the project. It is used to generate the right index name
        """
        self._es = es
        self._project_name = project_name

    def get(self) -> None:
        """
        This method is not allowed
        :return 405 HTTP error:
        """
        abort(405, message="method not allowed")

    def post(self) -> tuple:
        """
        Add a new single log message to the database. The log must be passed in the request body.
        This method log the messages with index <project-name>-logging-<yyyy>-<mm>-<dd
        :return: the HTTP response
        """
        data = request.get_json()
        utils = Utils()
        # TODO check structure in v 0.0.4
        trace_id = utils._extract_trace_id(data)
        logging.warning("INFO@LogGeneralLog POST - starting to log a new log with id [%s] at [%s]" % (
            trace_id, str(datetime.now())))

        index_name = self._project_name + "-logging-" + datetime.today().strftime('%Y-%m-%d')

        self._es.index(index=index_name, doc_type='_doc', body=data)

        response_json = {
            "trace_id": trace_id,
            "status": "ok",
            "code": 200
        }

        logging.warning("INFO@LogGeneralLog POST - finishing to log a new log with id [%s] at [%s]" % (
            trace_id, str(datetime.now())))

        return response_json, 200


class LogGeneralLogs(Resource):
    """
    This class can be used to log an array of log messages.
    """

    def __init__(self, es, project_name):
        """
        Function to gather the external parameters that are going to be used in the other methods
        :param es: the elasticsearch instance
        :param project_name: the name of the project
        """
        self._es = es
        self._project_name = project_name

    def get(self) -> None:
        """
        This method is not allowed
        :return: 405 HTTTP Error
        """
        abort(405, message="method not allowed")

    def post(self) -> tuple:
        """
        Add a batch of log messages to the database. The logs must be passed in the request body.
        This method log the messages with index <project-name>-logging-<yyyy>-<mm>-<dd>
        :return: the HTTP response
        """
        logging.warning(
            "INFO@LogGeneralLogs POST - starting to log an array of logs at [%s]" % (str(datetime.now())))
        messages_received = request.json
        message_ids = []
        for element in messages_received:
            # push the message in the database
            utils = Utils()
            # TODO check structure in v 0.0.4
            trace_id = utils._extract_trace_id(element)
            logging.warning("INFO@LogGeneralLogs POST - starting to log a new log with id [%s] at [%s]" % (
                trace_id, str(datetime.now())))

            index_name = self._project_name + "-logging-" + datetime.today().strftime('%Y-%m-%d')

            self._es.index(index=index_name, doc_type='_doc', body=element)

            message_ids.append(trace_id)

            logging.warning("INFO@LogGeneralLogs POST - finishing to log a new log with id [%s] at [%s]" % (
                trace_id, str(datetime.now())))
        json_response = {
            "ids_logged": ';'.join(message_ids),
            "status": "ok",
            "code": 200
        }
        logging.warning(
            "INFO@LogGeneralLogs POST - finishing to log an array of logs at [%s]" % (str(datetime.now())))
        return json_response, 200
