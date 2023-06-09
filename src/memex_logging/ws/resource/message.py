# Copyright 2021 U-Hopper srl
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
from memex_logging.common.dao.common import DocumentNotFound
from memex_logging.common.model.message import Message


logger = logging.getLogger("logger.resource.message")


class MessageResourceBuilder(object):

    @staticmethod
    def routes(dao_collector: DaoCollector):
        return [
            (MessageInterface, '/message', (dao_collector,)),
            (MessagesInterface, '/messages', (dao_collector,)),
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
            message, trace_id = self._dao_collector.message.get(project=project, message_id=message_id, user_id=user_id, trace_id=trace_id)
        except ValueError as e:
            logger.debug("Missing required parameters", exc_info=e)
            return {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }, 400
        except DocumentNotFound as e:
            logger.debug("Resource not found", exc_info=e)
            return {
                "status": "Not found: resource not found",
                "code": 404
            }, 404
        except Exception as e:
            logger.exception("Message failed to be retrieved", exc_info=e)
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
            self._dao_collector.message.delete(project=project, message_id=message_id, user_id=user_id, trace_id=trace_id)
        except ValueError as e:
            logger.debug("Missing required parameters", exc_info=e)
            return {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }, 400
        except Exception as e:
            logger.exception("Message failed to be deleted", exc_info=e)
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
            logger.debug("Message failed to be logged due to missing data")
            return {
                "status": "Malformed request: data is missing",
                "code": 400
            }, 400

        trace_ids = []

        try:
            messages = [Message.from_repr(m_r) for m_r in messages_received]
        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logger.debug("Error while parsing input message data", exc_info=e)
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
                trace_id = self._dao_collector.message.add(message)
                trace_ids.append(trace_id)
            except Exception as e:
                logger.exception(f"Could not save message with id {message.message_id} could not be saved", exc_info=e)
                logger.error(message)
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

        project = request.args.get('project')
        if project is None:
            logger.debug("Missing required `project` parameter")
            return {
                "status": "Malformed request: missing required `project` parameter",
                "code": 400
            }, 400

        try:
            from_time = datetime.fromisoformat(request.args.get('fromTime'))
            to_time = datetime.fromisoformat(request.args.get('toTime'))
        except Exception as e:
            logger.debug("Cannot parse `fromTime` and the `toTime` correctly", exc_info=e)
            return {
                "status": "Malformed request: missing required parameter, you have to specify the `fromTime` and the `toTime` in ISO format",
                "code": 400
            }, 400

        user_id = request.args.get('userId', None)
        channel = request.args.get('channel', None)
        message_type = request.args.get('type', None)

        try:
            max_size = int(request.args.get('maxSize', 1000))
        except Exception as e:
            logger.debug("`maxSize` is not an integer value", exc_info=e)
            return {
                "status": "Malformed request: `maxSize` is not an integer value",
                "code": 400
            }, 400

        if max_size > 10000:
            logger.debug("`maxSize` is too large, must be less than or equal to: [10000] but was [{max_size}]")
            return {
                "status": f"Malformed request: `maxSize` is too large, must be less than or equal to: [10000] but was [{max_size}]",
                "code": 400
            }, 400

        try:
            messages = self._dao_collector.message.search(project, from_time, to_time, max_size, user_id=user_id, channel=channel, message_type=message_type)
        except ValueError as e:
            logger.debug("`fromTime` is greater than `toTime`", exc_info=e)
            return {
                "status": "Malformed request: `fromTime` is greater than `toTime`",
                "code": 400
            }, 400
        except Exception as e:
            logger.exception("Message failed to be retrieved", exc_info=e)
            return {
                "status": "Internal server error: could not retrieve the message",
                "code": 500
            }, 500

        return [message.to_repr() for message in messages], 200

    def delete(self):
        """
        Delete a messages for an user.
        """


        try:
            user_id = request.args['userId']
            logger.info(f"Cleaning messages for user [{user_id}]")
            self._dao_collector.message.delete_by_user(user_id)
        except (ValueError, KeyError) as e:
            logger.debug("Missing required parameters", exc_info=e)
            return {
                "status": "Malformed request: missing required parameter, you have to specify only the `traceId` or the `project`, the `messageId` and the `userId`",
                "code": 400
            }, 400
        except Exception as e:
            logger.exception("Message failed to be deleted", exc_info=e)
            return {
                "status": "Internal server error: could not delete the message",
                "code": 500
            }, 500

        return {
            "status": "Ok: message deleted",
            "code": 200
        }, 200