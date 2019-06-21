from __future__ import absolute_import

import json

from flask import request, Response
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

    def __init__(self, es: Elasticsearch) -> None:
        self._es = es

    def delete(self, project: str, messageId: str) -> Response:
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
            "messageId": messageId,
            "action": "deleted",
            "code": 200
        }
        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200

        return resp

    def get(self, project, messageId)-> Response:
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
            resp = Response(json.dumps(response['hits']['hits'][0]['_source']), mimetype='application/json')
            resp.status_code = 200

            return resp


class LogMessages(Resource):
    """
    This class can be used to log an array of messages. The only method allowed is post
    This method log the messages with index <project-name>-message-<yyyy>-<mm>-<dd
    """
    def __init__(self, es: Elasticsearch):
        self._es = es

    def post(self) -> Response:
        """
        Add a batch of messages to the database. Pass a JSON array in the body of the request
        :return: the HTTP response
        """
        messages_received = request.json
        message_ids = []

        for message in messages_received:
            type = str(message['type']).lower()
            utils = Utils()

            if type == 'request':
                request_message = RequestMessage.from_rep(message)
                message_ids.append(request_message.message_id)
                project_name = utils.extract_project_name(message)
                date = datetime.today().strftime('%Y-%m-%d')
                index_name = project_name + "-message-" + date
                self._es.index(index=index_name, doc_type='_doc', body=request_message.to_repr())

            elif type == "response":
                response_message = ResponseMessage.from_rep(message)
                message_ids.append(response_message.message_id)
                project_name = utils.extract_project_name(message)
                index_name = project_name + "-message-" + datetime.today().strftime('%Y-%m-%d')
                self._es.index(index=index_name, doc_type='_doc', body=response_message.to_repr())

            elif type == "notification":
                notification_message = NotificationMessage.from_rep(message)
                message_ids.append(notification_message.message_id)
                project_name = utils.extract_project_name(message)
                index_name = project_name + "-message-" + datetime.today().strftime('%Y-%m-%d')
                self._es.index(index=index_name, doc_type='_doc', body=notification_message.to_repr())

        json_response = {
            "messageId": message_ids,
            "status": "ok",
            "code": 200
        }

        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200

        return resp
