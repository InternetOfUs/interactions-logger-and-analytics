from flask_restful import Resource, abort
# for handling elasticsearch
from elasticsearch import Elasticsearch
# for managing the responses
import json

class PerformancesResourceBuilder:

    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (GetMessage, '/message/<string:trace_id>', (es,)),
            (GetConversation, '/conversation/<string:conversation_id>', (es,))
        ]


class GetMessage(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self, trace_id)->tuple:
        """
        Retrieve a single message by specifying the trace id
        :param trace_id: the id of the message to retrieve
        :return: the HTTP response
        """

        response = self._es.search(index="memex-*", body={"query": {"match": {"traceId": trace_id}}})

        if response['hits']['total'] == 0:
            abort(404, message="resource not found")
        else:
            return response['hits']['hits'][0]['_source'], 200

    def post(self)-> None:
        abort(405, message="method not allowed")


class GetConversation(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self, conversation_id) -> tuple:
        """
        Retrieve a conversation by specifying the conversation id
        :return: the HTTP response
        """

        response = self._es.search(index="memex-*", body={"query": {"term": {"conversationId.keyword": {"value": conversation_id}}}})

        if response['hits']['total'] == 0:
            abort(404, message="resource not found")
        else:
            return response['hits']['hits'], 200

    def post(self) -> None:
        abort(405, message="method not allowed")
