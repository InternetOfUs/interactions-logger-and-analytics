from flask import Flask, request
from flask_restful import Resource, Api, abort
# for second-level logging
import logging
from datetime import datetime
# for handling elasticsearch
from elasticsearch import Elasticsearch
from memex_logging.utils.utils import Utils


class MessageResourceBuilder(object):

    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (LogMessage, '/message', (es,)),
            (LogMessages, '/messages', (es,))
        ]


class LogMessage(Resource):
    """
    This class can be used to log a single message. The only message allowed is post
    """

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self) -> None:
        abort(405, message="method not allowed")

    def post(self) -> tuple:
        """
        Add a new single message to the database. Pass a single message in JSON format as part of the body of the request
        This method log the messages with index <project-name>-message-<yyyy>-<mm>-<dd
        :return: the HTTP response
        """
        data = request.get_json()
        utils = Utils()
        # TODO check structure in v 0.0.4
        trace_id = utils.extract_trace_id(data)
        logging.warning("INFO@LogMessage POST - starting to log a new message with id [%s] at [%s]" % (
            trace_id, str(datetime.now())))
        conversation_id = utils.compute_conversation_id()
        data["conversationId"] = conversation_id

        project_name = utils.extract_project_name(data)

        index_name = project_name + "-message-" + datetime.today().strftime('%Y-%m-%d')

        self._es.index(index=index_name, doc_type='_doc', body=data)
        response_json = {
            "trace_id": trace_id,
            "status": "ok",
            "code": 200
        }

        logging.warning("INFO@LogMessage POST - finishing to log a new message with id [%s] at [%s]" % (
            trace_id, str(datetime.now())))
        return response_json, 200


class LogMessages(Resource):
    """
    This class can be used to log an array of messages. The only method allowed is post
    This method log the messages with index <project-name>-message-<yyyy>-<mm>-<dd
    """

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self) -> None:
        abort(405, message="method not allowed")

    def post(self) -> tuple:
        """
        Add a batch of messages to the database. Pass a JSON array in the body of the request
        :return: the HTTP response
        """
        logging.warning(
            "INFO@LogMessages POST - starting to log an array of messages at [%s]" % (str(datetime.now())))
        messages_received = request.json
        message_ids = []
        for element in messages_received:
            # push the message in the database
            utils = Utils()
            # TODO check structure in v 0.0.4
            trace_id = utils.extract_trace_id(element)
            logging.warning("INFO@LogMessage POST - starting to log a new message with id [%s] at [%s]" % (
                trace_id, str(datetime.now())))
            conversation_id = utils.compute_conversation_id()
            element["conversationId"] = conversation_id

            project_name = utils.extract_project_name(element)

            index_name = project_name + "-message-" + datetime.today().strftime('%Y-%m-%d')

            self._es.index(index=index_name, doc_type='_doc', body=element)
            message_ids.append(trace_id)
            logging.warning("INFO@LogMessage POST - finishing to log a new message with id [%s] at [%s]" % (
                trace_id, str(datetime.now())))
        json_response = {
            "ids_logged": ';'.join(message_ids),
            "status": "ok",
            "code": 200
        }
        logging.warning(
            "INFO@LogMessages POST - finishing to log an array of messages at [%s]" % (str(datetime.now())))
        return json_response, 200
