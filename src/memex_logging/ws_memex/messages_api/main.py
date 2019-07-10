from __future__ import absolute_import

import json

from flask import request, Response
from flask_restful import Resource, abort

from datetime import datetime

from elasticsearch import Elasticsearch

from memex_logging.models.message import RequestMessage, ResponseMessage, NotificationMessage
from memex_logging.utils.utils import Utils

import logging

class MessageResourceBuilder(object):

    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (ManipulateMessage, '/message/', (es,)),
            (LogMessages, '/messages', (es,))
        ]


class ManipulateMessage(Resource):
    """
    This class can be used to log a single message. The only message allowed is post
    """

    def __init__(self, es: Elasticsearch) -> None:
        self._es = es

    def delete(self) -> Response:
        """
        Method to delete a message of a specific project by id
        :param project: the name of the project
        :param messageId: the id of the message
        :return: the HTTP response
        """
        project = request.args.get('project')
        messageId = request.args.get('messageId')

        logging.warning("MESSAGES.API Starting to delete a message in project {} with id {}".format(project, messageId))

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

    def get(self)-> Response:
        """
        Method to obtain a message of a specific project by id
        :return: the HTTP response
        """
        if 'project' in request.args and 'messageId' in request.args:
            project = request.args.get('project')
            message_id = request.args.get('messageId')
            is_generic = 0
        elif 'id' in request.args:
            message_id = request.args.get("id")
            is_generic = 1
        else:
            logging.error("MESSAGES.API cannot parse query parameters correctly while elaborating a get")

        if is_generic:
            logging.warning("MESSAGES.API Starting to look up for a message with id {}".format(message_id))
            index = "*"
            query = {
                "query": {
                    "match": {
                        "_id": message_id
                    }
                }
            }
        else:
            logging.warning("MESSAGES.API Starting to look up for a message in project {} with id {}".format(project, message_id))
            index = str(project).lower() + "-message-*"
            query = {
                "query": {
                    "match": {
                        "messageId": message_id
                    }
                }
            }

        response = self._es.search(index=index, body=query)
        if response['hits']['total'] == 0:
            logging.error("No resource found for id {}".format(message_id))
            abort(404, message="resource not found")
        else:
            logging.info("Succeed in retrieving the message")
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

        logging.warning("MESSAGES.API Starting to log a new set of messages")

        messages_received = request.json
        message_ids = []

        for message in messages_received:

            type = str(message['type']).lower()
            utils = Utils()

            id = str(message['messageId'])

            if type == 'request':
                try:
                    request_message = RequestMessage.from_rep(message)
                    project_name = utils.extract_project_name(message)
                    date = utils.extract_date(message)
                    index_name = project_name + "-message-" + date
                    query = self._es.index(index=index_name, doc_type='_doc', body=request_message.to_repr())
                    message_ids.append(query['_id'])
                except:
                    logging.error("MESSAGES.API request message with id {} failed to be logged".format(id))
                    logging.error(message)
            elif type == "response":
                try:
                    response_message = ResponseMessage.from_rep(message)
                    project_name = utils.extract_project_name(message)
                    date = utils.extract_date(message)
                    index_name = project_name + "-message-" + date
                    query = self._es.index(index=index_name, doc_type='_doc', body=response_message.to_repr())
                    message_ids.append(query['_id'])
                except:
                    logging.error("MESSAGES.API response message with id {} failed to be logged".format(id))
                    logging.error(message)
            elif type == "notification":
                try:
                    notification_message = NotificationMessage.from_rep(message)
                    project_name = utils.extract_project_name(message)
                    date = utils.extract_date(message)
                    index_name = project_name + "-message-" + date
                    query = self._es.index(index=index_name, doc_type='_doc', body=notification_message.to_repr())
                    message_ids.append(query['_id'])
                except:
                    logging.error("MESSAGES.API request message with id {} failed to be logged".format(id))
                    logging.error(message)

        json_response = {
            "messageId": message_ids,
            "status": "ok",
            "code": 200
        }

        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200

        return resp
