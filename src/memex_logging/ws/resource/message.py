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

from memex_logging.common.model.message import RequestMessage, ResponseMessage, NotificationMessage, Message
from memex_logging.utils.utils import Utils


logger = logging.getLogger("logger.messages_api")


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

    def delete(self) -> Response:
        """
        Delete a specific message.
        """

        if 'project' in request.args and 'messageId' in request.args:
            project = request.args.get('project')
            message_id = request.args.get('messageId')
            logging.warning("DELETE request, starting to delete a message in project {} with messageId {}".format(project, message_id))
            index = "message-" + str(project).lower() + "-*"
            query = {
                "query": {
                    "match": {
                        "messageId": message_id
                    }
                }
            }
        else:
            logging.error("DELETE request, cannot parse parameters correctly while elaborating the delete request")
            json_response = {
                "status": "Malformed request: missing required parameter, you have to specify `messageId` and the `project`",
                "code": 400
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 400
            return resp

        try:
            self._es.delete_by_query(index=index, body=query)
            # self._es.delete(index=index, id=trace_id)
        except Exception as e:
            logging.exception("DELETE request, message failed to be deleted", exc_info=e)
            json_response = {
                "status": "Internal server error: could not delete messages",
                "code": 500
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 500
            return resp

        json_response = {
            "messageId": message_id,
            "action": "deleted",
            "code": 200
        }
        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200
        return resp

    def get(self) -> Response:
        """
        Get details of a message.
        """

        if 'project' in request.args and 'messageId' in request.args:
            project = request.args.get('project')
            message_id = request.args.get('messageId')
            logging.warning("GET request, starting to look up for a message in project {} with messageId {}".format(project, message_id))
            index = "message-" + str(project).lower() + "-*"
            query = {
                "query": {
                    "match": {
                        "messageId": message_id
                    }
                }
            }
        elif 'traceId' in request.args:
            trace_id = request.args.get("traceId")
            logging.warning("GET request, starting to look up for a message with traceId {}".format(trace_id))
            index = "*"
            query = {
                "query": {
                    "match": {
                        "_id": trace_id
                    }
                }
            }
        else:
            logging.error("GET request, cannot parse parameters correctly while elaborating the get request")
            json_response = {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `messageId` and the `project`",
                "code": 400
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 400
            return resp

        try:
            response = self._es.search(index=index, body=query)
            # response = self._es.get(index=index, id=trace_id)
        except Exception as e:
            logging.exception("GET request, message failed to be retrieved", exc_info=e)
            json_response = {
                "status": "Internal server error: could not delete messages",
                "code": 500
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 500
            return resp

        if len(response['hits']['hits']) == 0:
            json_response = {
                "status": "Resource not found",
                "code": 404
            }
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 404
            return resp
        else:
            resp = Response(json.dumps(response['hits']['hits'][0]['_source']), mimetype='application/json')
            resp.status_code = 200
            return resp


class MessagesInterface(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def post(self) -> Response:
        """
        Register a batch of messages.
        """

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

        try:
            messages = [Message.from_repr(m_r) for m_r in messages_received]
        except (KeyError, ValueError) as e:
            logger.exception("Error while parsing input message data", exc_info=e)
            return {
                "message": "Could not parse malformed data"
            }, 400
        except Exception as e:
            logger.exception("Something went wrong while parsing message list", exc_info=e)
            return {
                "message": "Something went wrong"
            }, 500

        for message in messages:
            try:
                index_name = Utils.generate_index(message.project, "message", message.timestamp)
                query = self._es.index(index=index_name, doc_type='_doc', body=message.to_repr())
                trace_ids.append(query['_id'])
            except Exception as e:
                logging.exception(f"Could not save message with id {message.id} could not be saved", exc_info=e)
                logging.error(message)
                return {
                    "status": "Internal server error: could not store messages",
                    "code": 500
                }, 500

        return {
            "traceIds": trace_ids,
            "status": "ok",
            "code": 201
        }, 201
