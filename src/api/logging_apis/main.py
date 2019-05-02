from flask import Flask, request
from flask_restful import Resource, Api, abort
# for second-level logging
import logging
import datetime
# for handling elasticsearch
from elasticsearch import Elasticsearch
import argparse
# for managing the responses
import json

from api.utils.utils import Utils

argParser = argparse.ArgumentParser()
argParser.add_argument("-hs", "--host", type=str, dest="h", default="localhost")
argParser.add_argument("-p", "--port", type=str, dest="p", default="9200")
args = argParser.parse_args()

app = Flask(__name__)
api = Api(app)

es = Elasticsearch([{'host': args.h, 'port': args.p}])


class LogMessage(Resource):

    def get(self) -> None:
        abort(405, message="method not allowed")

    def post(self) -> dict:
        """
        Add a new single message to the database. Pass a single message in JSON foramt as part of the body of the request
        :return: the HTTP response
        """
        data = request.get_json()
        utils = Utils()
        trace_id = utils._extract_trace_id(data)
        logging.warning("INFO@LogMessage POST - starting to log a new message with id [%s] at [%s]" % (trace_id, str(datetime.datetime.now())) )
        conversation_id = utils._compute_conversation_id()
        data["conversationId"] = conversation_id
        es.index(index='conversation-message', doc_type='_doc', body=data)
        response_json = {
            "trace_id": trace_id,
            "status": "ok",
            "code": 200
        }
        response = app.response_class(
            response=json.dumps(response_json),
            status=200,
            mimetype="application/json"
        )
        logging.warning("INFO@LogMessage POST - finishing to log a new message with id [%s] at [%s]" % (trace_id, str(datetime.datetime.now())))
        return response


class LogMessages(Resource):

    def get(self) -> None:
        abort(405, message="method not allowed")

    def post(self) -> dict:
        """
        Add a batch of messages to the database. Pass a JSON array in the body of the request
        :return: the HTTP response
        """
        logging.warning("INFO@LogMessages POST - starting to log an array of messages at [%s]" % (str(datetime.datetime.now())))
        messages_received = request.json
        for element in messages_received:
            # push the message in the database
            utils = Utils()
            trace_id = utils._extract_trace_id(element)
            logging.warning("INFO@LogMessage POST - starting to log a new message with id [%s] at [%s]" % (trace_id, str(datetime.datetime.now())))
            conversation_id = utils._compute_conversation_id()
            element["conversationId"] = conversation_id
            es.index(index='conversation-message', doc_type='_doc', body=element)
            response_json = {
                "trace_id": trace_id,
                "status": "ok",
                "code": 200
            }
            response = app.response_class(
                response=json.dumps(response_json),
                status=200,
                mimetype="application/json"
            )
            logging.warning("INFO@LogMessage POST - finishing to log a new message with id [%s] at [%s]" % (trace_id, str(datetime.datetime.now())))

        response = app.response_class(
            response='Ok',
            status=200,
            mimetype='application/json'
        )
        logging.warning(
            "INFO@LogMessages POST - finishing to log an array of messages at [%s]" % (str(datetime.datetime.now())))
        return response


api.add_resource(LogMessage, '/LogMessage')
api.add_resource(LogMessages, '/LogMessages')

app.run(host="0.0.0.0", port=5000, debug=True)
