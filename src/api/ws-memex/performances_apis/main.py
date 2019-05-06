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

argParser = argparse.ArgumentParser()
argParser.add_argument("-hs", "--host", type=str, dest="h", default="localhost")
argParser.add_argument("-p", "--port", type=str, dest="p", default="9200")
args = argParser.parse_args()

app = Flask(__name__)
api = Api(app)

es = Elasticsearch([{'host': args.h, 'port': args.p}])


class GetMessage(Resource):

    def get(self)->dict:
        """
        Retrieve a single message by specifying the trace id
        :param traceId: the id of the message to retrieve
        :return: the HTTP response
        """

        trace_id = request.args['traceId']

        response = es.search(index="conversation-message", body={"query": {"match": {"traceId": trace_id}}})

        if response['hits']['total'] == 0:
            abort(404, message="resource not found")
        else:
            response = app.response_class(
                response=json.dumps(response['hits']['hits'][0]['_source']),
                status=200,
                mimetype='application/json'
            )
            return response

    def post(self)-> None:
        abort(405, message="method not allowed")


class GetConversation(Resource):

    def get(self) -> dict:
        """
        Retrieve a conversation by specifying the conversation id
        :return: the HTTP response
        """
        conversation_id = request.args.get

        response = es.search(index="mem-ex",
                             body={"query": {"term": {"conversationId.keyword": {"value": conversation_id}}}})

        if response['hits']['total'] == 0:
            abort(404, message="resource not found")
            return response
        else:
            response = app.response_class(
                response=json.dumps(response['hits']['hits']),
                status=200,
                mimetype='application/json'
            )
            return response

    def post(self) -> None:
        abort(405, message="method not allowed")


class GetServiceList(Resource):

    def get(self) -> dict:
        # TODO define where to find the list of services in the model
        return {}

    def post(self) -> None:
        abort(405, message="method not allowed")


class GetLowPerformancesModules(Resource):

    def get(self) -> dict:
        # TODO defined where to find the performances in the model
        return {}

    def post(self) -> None:
        abort(405, message="method not allowed")


api.add_resource(GetMessage, '/GetMessage')
api.add_resource(GetConversation, '/GetConversation')
api.add_resource(GetServiceList, '/GetServiceList')
api.add_resource(GetLowPerformancesModules, '/GetLowPerformances')

app.run()
