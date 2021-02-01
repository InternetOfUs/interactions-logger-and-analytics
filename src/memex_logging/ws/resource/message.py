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

import logging

from elasticsearch import Elasticsearch
from flask import request
from flask_restful import Resource

from memex_logging.common.model.message import Message
from memex_logging.utils.utils import Utils


logger = logging.getLogger("logger.resource.message")


class MessageResourceBuilder(object):

    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (MessageInterface, '/message', (es,)),
            (MessagesInterface, '/messages', (es,))
        ]


class MessageInterface(Resource):

    def __init__(self, es: Elasticsearch) -> None:
        self._es = es

    def delete(self):
        """
        Delete a specific message.
        """

        if 'project' in request.args and 'messageId' in request.args and 'userId' in request.args:
            project = request.args.get('project')
            message_id = request.args.get('messageId')
            user_id = request.args.get('userId')
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
            logging.error("Cannot parse parameters correctly while elaborating the delete request")
            return {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }, 400

        try:
            self._es.delete_by_query(index=index, body=query)
        except Exception as e:
            logging.exception("Message failed to be deleted", exc_info=e)
            return {
                "status": "Internal server error: could not delete the message",
                "code": 500
            }, 500

        return {
            "status": "Ok: message deleted",
            "code": 200
        }, 200

    def get(self):
        """
        Get details of a message.
        """

        if 'project' in request.args and 'messageId' in request.args and 'userId' in request.args:
            project = request.args.get('project')
            message_id = request.args.get('messageId')
            user_id = request.args.get('userId')
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
            logging.error("Cannot parse parameters correctly while elaborating the get request")
            return {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }, 400

        try:
            response = self._es.search(index=index, body=query)
        except Exception as e:
            logging.exception("Message failed to be retrieved", exc_info=e)
            return {
                "status": "Internal server error: could not retrieve the message",
                "code": 500
            }, 500

        if len(response['hits']['hits']) == 0:
            return {
                "status": "Not found: resource not found",
                "code": 404
            }, 404
        else:
            json_response = response['hits']['hits'][0]['_source']
            json_response["traceId"] = response['hits']['hits'][0]['_id']
            return json_response, 200


class MessagesInterface(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def post(self):
        """
        Register a batch of messages.
        """

        messages_received = request.json
        if messages_received is None:
            logging.error("Message failed to be logged due to missing data")
            return {
                "status": "Malformed request: data is missing",
                "code": 400
            }, 400

        trace_ids = []

        try:
            messages = [Message.from_repr(m_r) for m_r in messages_received]
        except (KeyError, ValueError, TypeError) as e:
            logger.exception("Error while parsing input message data", exc_info=e)
            return {
                "status": "Malformed request: could not parse malformed data",
                "code": 400
            }, 400
        except Exception as e:
            logger.exception("Something went wrong while parsing message list", exc_info=e)
            return {
                "status": "Internal server error: something went wrong in parsing messages",
                "code": 500
            }, 500

        for message in messages:
            try:
                index_name = Utils.generate_index(message.project, "message", message.timestamp)
                query = self._es.index(index=index_name, doc_type='_doc', body=message.to_repr())
                trace_ids.append(query['_id'])
            except Exception as e:
                logging.exception(f"Could not save message with id {message.message_id} could not be saved", exc_info=e)
                logging.error(message)
                return {
                    "status": "Internal server error: something went wrong in storing messages",
                    "code": 500
                }, 500

        return {
            "traceIds": trace_ids,
            "status": "Created: messages stored",
            "code": 201
        }, 201
