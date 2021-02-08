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

from flask import request
from flask_restful import Resource

from memex_logging.common.dao.collector import DaoCollector
from memex_logging.common.dao.conmon import EntryNotFound
from memex_logging.common.model.message import Message


logger = logging.getLogger("logger.resource.message")


class MessageResourceBuilder(object):

    @staticmethod
    def routes(dao_collector: DaoCollector):
        return [
            (MessageInterface, '/message', (dao_collector,)),
            (MessagesInterface, '/messages', (dao_collector,))
        ]


class MessageInterface(Resource):

    def __init__(self, dao_collector: DaoCollector) -> None:
        self._dao_collector = dao_collector

    def get(self):
        """
        Get details of a message.
        """

        project = request.args.get('project', None)
        message_id = request.args.get('messageId', None)
        user_id = request.args.get('userId', None)
        trace_id = request.args.get('traceId', None)

        try:
            message, trace_id = self._dao_collector.message_dao.get_message(project=project, message_id=message_id, user_id=user_id, trace_id=trace_id)
        except ValueError:
            logging.error("Missing required parameters")
            return {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }, 400
        except EntryNotFound:
            logger.debug("Resource not found")
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

        json_response = message.to_repr()
        json_response["traceId"] = trace_id
        return json_response, 200

    def delete(self):
        """
        Delete a specific message.
        """

        project = request.args.get('project', None)
        message_id = request.args.get('messageId', None)
        user_id = request.args.get('userId', None)
        trace_id = request.args.get('traceId', None)

        try:
            self._dao_collector.message_dao.delete_message(project=project, message_id=message_id, user_id=user_id, trace_id=trace_id)
        except ValueError:
            logging.error("Missing required parameters")
            return {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }, 400
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


class MessagesInterface(Resource):

    def __init__(self, dao_collector: DaoCollector) -> None:
        self._dao_collector = dao_collector

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
        except (KeyError, ValueError, TypeError, AttributeError) as e:
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
                trace_id = self._dao_collector.message_dao.add_messages(message)
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

        try:
            project = request.args.get('project')
            from_time = datetime.fromisoformat(request.args.get('fromTime'))
            to_time = datetime.fromisoformat(request.args.get('toTime'))
        except Exception as e:
            logging.error("Cannot parse parameters correctly while elaborating the get request", exc_info=e)
            return {
                "status": "Malformed request: missing required parameter, you have to specify the `project`, the `fromTime` (in ISO format) and the `toTime` (in ISO format)",
                "code": 400
            }, 400

        user_id = request.args.get('userId', None)
        channel = request.args.get('channel', None)
        message_type = request.args.get('type', None)

        try:
            messages = self._dao_collector.message_dao.search_messages(project, from_time, to_time, user_id=user_id, channel=channel, message_type=message_type)
        except ValueError:
            logger.error("`fromTime` is greater than `toTime`")
            return {
                "status": "Malformed request: `fromTime` is greater than `toTime`",
                "code": 400
            }, 400
        except Exception as e:
            logging.exception("Message failed to be retrieved", exc_info=e)
            return {
                "status": "Internal server error: could not retrieve the message",
                "code": 500
            }, 500

        return [message.to_repr() for message in messages], 200
