# Copyright 2020 U-Hopper srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, annotations

from elasticsearch import Elasticsearch
from flask_restful import Resource, abort


class PerformancesResourceBuilder:

    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (GetMessage, '/message/<string:trace_id>', (es,)),
            (GetConversation, '/conversation/<string:conversation_id>', (es,)),
            (DeleteIndex, '/index/<string:index_name>', (es,))
        ]


class GetMessage(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self, trace_id) -> tuple:
        """
        Retrieve a single message by specifying the trace id
        :param trace_id: the id of the message to retrieve
        :return: the HTTP response
        """

        response = self._es.search(index="message-*", body={"query": {"match": {"traceId": trace_id}}})

        if response['hits']['total'] == 0:
            abort(404, message="resource not found")
        else:
            return response['hits']['hits'][0]['_source'], 200

    def post(self) -> None:
        abort(405, message="method not allowed")


class GetConversation(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self, conversation_id) -> tuple:
        """
        Retrieve a conversation by specifying the conversation id
        :return: the HTTP response
        """

        response = self._es.search(index="message-*", body={"query": {"term": {"conversationId.keyword": {"value": conversation_id}}}})

        if response['hits']['total'] == 0:
            abort(404, message="resource not found")
        else:
            return response['hits']['hits'], 200

    def post(self) -> None:
        abort(405, message="method not allowed")


class DeleteIndex(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self):
        abort(405, message="method not allowed")

    def post(self):
        abort(405, message="method not allowed")

    def delete(self, index_name):

        if index_name == "":
            # TODO check code
            abort(400, message="")
