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
from datetime import datetime

from elasticsearch import Elasticsearch
from flask import request
from flask_restful import Resource

from memex_logging.common.dao.conmon import EntryNotFound
from memex_logging.common.dao.message import MessageDao
from memex_logging.common.model.message import Message


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
        self._message_dao = MessageDao(es)

    def delete(self):
        """
        Delete a specific message.
        """

        if 'project' in request.args and 'messageId' in request.args and 'userId' in request.args:
            project = request.args.get('project')
            message_id = request.args.get('messageId')
            user_id = request.args.get('userId')
            index = self._message_dao.generate_index(project=project)
            query = self._message_dao.build_query_by_message_id(message_id)
            query = self._message_dao.add_user_id_to_query(query, user_id)

        elif 'traceId' in request.args:
            trace_id = request.args.get("traceId")
            index = self._message_dao.generate_index()
            query = self._message_dao.build_query_by_id(trace_id)

        else:
            logging.error("Cannot parse parameters correctly while elaborating the delete request")
            return {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }, 400

        try:
            self._message_dao.delete(index, query)
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
            index = self._message_dao.generate_index(project=project)
            query = self._message_dao.build_query_by_message_id(message_id)
            query = self._message_dao.add_user_id_to_query(query, user_id)

        elif 'traceId' in request.args:
            trace_id = request.args.get("traceId")
            index = self._message_dao.generate_index()
            query = self._message_dao.build_query_by_id(trace_id)

        else:
            logging.error("Cannot parse parameters correctly while elaborating the get request")
            return {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }, 400

        try:
            response = self._message_dao.get(index, query)
        except EntryNotFound:
            return {
                       "status": "Not found: resource not found",
                       "code": 404
                   }, 404
        except Exception as e:
            logging.exception("Message failed to be retrieved", exc_info=e)
            return {
                "status": "Internal server error: could not retrieve the message",
                "code": 500
            }, 500

        json_response = response[0].to_repr()
        json_response["traceId"] = response[1]
        return json_response, 200


class MessagesInterface(Resource):

    def __init__(self, es: Elasticsearch):
        self._message_dao = MessageDao(es)

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
                index = self._message_dao.generate_index(message.project, message.timestamp)
                trace_id = self._message_dao.add(index, message)
                trace_ids.append(trace_id)
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

    def get(self):
        """
        Retrieve a list of messages.
        """

        if 'project' in request.args and 'fromTime' in request.args and 'toTime' in request.args:
            project = request.args.get('project')
            from_time = datetime.fromisoformat(request.args.get('fromTime'))
            to_time = datetime.fromisoformat(request.args.get('toTime'))

            if from_time > to_time:
                return {
                           "status": "Malformed request: `fromTime` is greater than `toTime`",
                           "code": 400
                       }, 400
            elif from_time.date() == to_time.date():
                index = self._message_dao.generate_index(project=project, dt=from_time)
            else:
                index = self._message_dao.generate_index(project=project)

            query = self._message_dao.build_time_range_query(from_time, to_time)
        else:
            logging.error("Cannot parse parameters correctly while elaborating the get request")
            return {
                "status": "Malformed request: missing required parameter, you have to specify the `project`, the `fromTime` and the `toTime`",
                "code": 400
            }, 400

        if 'userId' in request.args:
            user_id = request.args.get('userId')
            query = self._message_dao.add_user_id_to_query(query, user_id)

        if 'channel' in request.args:
            channel = request.args.get('channel')
            query = self._message_dao.add_channel_to_query(query, channel)

        if 'type' in request.args:
            message_type = request.args.get('type')
            query = self._message_dao.add_message_type_to_query(query, message_type)

        try:
            messages = self._message_dao.search(index, query)
        except Exception as e:
            logging.exception("Message failed to be retrieved", exc_info=e)
            return {
                "status": "Internal server error: could not retrieve the message",
                "code": 500
            }, 500

        return [message.to_repr() for message in messages], 200
