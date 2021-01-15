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

import json
import logging

from elasticsearch import Elasticsearch
from flask import request, Response
from flask_restful import Resource

from memex_logging.models.message import RequestMessage, ResponseMessage, NotificationMessage
from memex_logging.utils.utils import Utils


logger = logging.getLogger("logger.messages_api")


class MessageResourceBuilder(object):

    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (ManipulateMessage, '/message', (es,)),
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
        :return: the HTTP response
        """

        if 'project' in request.args and 'messageId' in request.args and 'userId' in request.args:
            project = request.args.get('project')
            message_id = request.args.get('messageId')
            user_id = request.args.get('userId')
            logging.warning("DELETE request, starting to delete a message in project {} with messageId {} and userId {}".format(project, message_id, user_id))
            index = "message-" + str(project).lower() + "-*"
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match_phrase": {
                                    "messageId": message_id
                                }
                            },
                            {
                                "match_phrase": {
                                    "userId": user_id
                                }
                            }
                        ]
                    }
                }
            }

        elif 'traceId' in request.args:
            trace_id = request.args.get("traceId")
            logging.warning("DELETE request, starting to delete a message with traceId {}".format(trace_id))
            index = "message-*"
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match_phrase": {
                                    "_id": trace_id
                                }
                            }
                        ]
                    }
                }
            }

        else:
            logging.error("DELETE request, cannot parse parameters correctly while elaborating the delete request")
            json_response = {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 400
            return resp

        try:
            self._es.delete_by_query(index=index, body=query)
        except Exception as e:
            logging.exception("DELETE request, message failed to be deleted", exc_info=e)
            json_response = {
                "status": "Internal server error: could not delete the message",
                "code": 500
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 500
            return resp

        json_response = {
            "status": "Ok: message deleted",
            "code": 200
        }
        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200
        return resp

    def get(self) -> Response:
        """
        Method to obtain a message of a specific project by id
        :return: the HTTP response
        """

        if 'project' in request.args and 'messageId' in request.args and 'userId' in request.args:
            project = request.args.get('project')
            message_id = request.args.get('messageId')
            user_id = request.args.get('userId')
            logging.warning("GET request, starting to look up for a message in project {} with messageId {} and userId {}".format(project, message_id, user_id))
            index = "message-" + str(project).lower() + "-*"
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match_phrase": {
                                    "messageId": message_id
                                }
                            },
                            {
                                "match_phrase": {
                                    "userId": user_id
                                }
                            }
                        ]
                    }
                }
            }

        elif 'traceId' in request.args:
            trace_id = request.args.get("traceId")
            logging.warning("GET request, starting to look up for a message with traceId {}".format(trace_id))
            index = "message-*"
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match_phrase": {
                                    "_id": trace_id
                                }
                            }
                        ]
                    }
                }
            }

        else:
            logging.error("GET request, cannot parse parameters correctly while elaborating the get request")
            json_response = {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 400
            return resp

        try:
            response = self._es.search(index=index, body=query)
        except Exception as e:
            logging.exception("GET request, message failed to be retrieved", exc_info=e)
            json_response = {
                "status": "Internal server error: could not retrieve the message",
                "code": 500
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 500
            return resp

        if len(response['hits']['hits']) == 0:
            json_response = {
                "status": "Not found: resource not found",
                "code": 404
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 404
            return resp
        else:
            json_response = response['hits']['hits'][0]['_source']
            json_response["traceId"] = response['hits']['hits'][0]['_id']
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 200
            return resp


class LogMessages(Resource):
    """
    This class can be used to log an array of messages. The only method allowed is post
    This method log the messages with index <project-name>-message-<yyyy>-<mm>-<dd>
    """

    def __init__(self, es: Elasticsearch):
        self._es = es

    def post(self) -> Response:
        """
        Add a batch of messages to the database. Pass a JSON array in the body of the request
        :return: the HTTP response
        """

        logging.warning("Starting to log a new set of messages")

        messages_received = request.json
        if messages_received is None:
            logging.error("POST request, message failed to be logged due to missing data")
            json_response = {
                "status": "Malformed request: data is missing",
                "code": 400
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 400
            return resp

        trace_ids = []

        for message in messages_received:

            try:
                message_type = str(message['type']).lower()
                message_id = str(message['messageId'])
                project_name = Utils.extract_project_name(message)
                date = Utils.extract_date(message)
                index_name = "message-" + project_name + "-" + date
            except KeyError as e:
                logging.exception("POST request, message failed to be logged due to malformed request", exc_info=e)
                logging.error(message)
                json_response = {
                    "status": "Malformed request: error in parsing messages",
                    "code": 400
                }
                resp = Response(json.dumps(json_response), mimetype='application/json')
                resp.status_code = 400
                return resp
            except Exception as e:
                logging.exception("POST request, message failed to be logged", exc_info=e)
                logging.error(message)
                json_response = {
                    "status": "Internal server error: could not parse messages",
                    "code": 500
                }
                resp = Response(json.dumps(json_response), mimetype='application/json')
                resp.status_code = 500
                return resp

            try:
                if message_type == "request":
                    request_message = RequestMessage.from_rep(message)
                    Utils.compute_conversation_id(self._es, request_message)
                    query = self._es.index(index=index_name, doc_type='_doc', body=request_message.to_repr())
                    trace_ids.append(query['_id'])
                elif message_type == "response":
                    response_message = ResponseMessage.from_rep(message)
                    query = self._es.index(index=index_name, doc_type='_doc', body=response_message.to_repr())
                    trace_ids.append(query['_id'])
                elif message_type == "notification":
                    notification_message = NotificationMessage.from_rep(message)
                    query = self._es.index(index=index_name, doc_type='_doc', body=notification_message.to_repr())
                    trace_ids.append(query['_id'])
            except KeyError as e:
                logging.exception("POST request, message with id {} failed to be logged due to malformed request".format(message_id), exc_info=e)
                logging.error(message)
                json_response = {
                    "status": "Malformed request: error in parsing messages",
                    "code": 400
                }
                resp = Response(json.dumps(json_response), mimetype='application/json')
                resp.status_code = 400
                return resp
            except Exception as e:
                logging.exception("POST request, message with id {} failed to be logged".format(message_id), exc_info=e)
                logging.error(message)
                json_response = {
                    "status": "Internal server error: could not store messages",
                    "code": 500
                }
                resp = Response(json.dumps(json_response), mimetype='application/json')
                resp.status_code = 500
                return resp

        json_response = {
            "traceIds": trace_ids,
            "status": "Created: messages stored",
            "code": 201
        }
        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 201
        return resp
