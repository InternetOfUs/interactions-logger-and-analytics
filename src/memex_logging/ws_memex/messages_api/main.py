from __future__ import absolute_import

from flask import request
from flask_restful import Resource, abort

from datetime import datetime

from elasticsearch import Elasticsearch

from memex_logging.models.message import RequestMessage, ResponseMessage, NotificationMessage
from memex_logging.utils.utils import Utils


class MessageResourceBuilder(object):

    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (ManipulateMessage, '/message/<string:project>/<string:messageId>', (es,)),
            (LogMessages, '/messages', (es,))
        ]


class ManipulateMessage(Resource):
    """
    This class can be used to log a single message. The only message allowed is post
    """

    def __init__(self, es: Elasticsearch):
        self._es = es

    def delete(self, project, messageId) -> tuple:
        """
        Method to delete a message of a specific project by id
        :param project: the name of the project
        :param messageId: the id of the message
        :return: the HTTP response
        """
        query = {
            "query": {
                "match": {
                    "messageId": messageId
                }
            }
        }
        index = str(project).lower() + "-message-*"
        self._es.delete_by_query(index=index, body=query)

        json_response = {
            "trace_id": messageId,
            "action": "deleted",
            "code": 200
        }
        return json_response, 200

    def get(self, project, messageId)-> tuple:
        """
        Method to obtain a message of a specific project by id
        :param project: the name of the project
        :param messageId: the id of the message
        :return: the HTTP response
        """
        query = {
            "query": {
                "match": {
                    "messageId": messageId
                }
            }
        }
        index = str(project).lower() + "-message-*"
        response = self._es.search(index=index, body=query)
        if response['hits']['total'] == 0:
            abort(404, message="resource not found")
        else:
            return response['hits']['hits'][0]['_source'], 200


class LogMessages(Resource):
    """
    This class can be used to log an array of messages. The only method allowed is post
    This method log the messages with index <project-name>-message-<yyyy>-<mm>-<dd
    """
    def __init__(self, es: Elasticsearch):
        self._es = es

    def post(self) -> tuple:
        """
        Add a batch of messages to the database. Pass a JSON array in the body of the request
        :return: the HTTP response
        """
        messages_received = request.json
        print(len(messages_received))
        for message in messages_received:
            type = str(message['type']).lower()
            utils = Utils()

            if type == 'request':
                request_message = RequestMessage.from_rep(message)
                project_name = utils.extract_project_name(message)
                index_name = project_name + "-message-" + datetime.today().strftime('%Y-%m-%d')
                self._es.index(index=index_name, doc_type='_doc', body=request_message.to_repr())

            elif type == "response":
                response_message = ResponseMessage.from_rep(message)
                project_name = utils.extract_project_name(message)
                index_name = project_name + "-message-" + datetime.today().strftime('%Y-%m-%d')
                self._es.index(index=index_name, doc_type='_doc', body=response_message.to_repr())

            elif type == "notification":
                notification_message = NotificationMessage.from_rep(message)
                project_name = utils.extract_project_name(message)
                index_name = project_name + "-message-" + datetime.today().strftime('%Y-%m-%d')
                self._es.index(index=index_name, doc_type='_doc', body=notification_message.to_repr())

        json_response = {
            "text": "messages added",
            "status": "ok",
            "code": 200
        }

        return json_response, 200