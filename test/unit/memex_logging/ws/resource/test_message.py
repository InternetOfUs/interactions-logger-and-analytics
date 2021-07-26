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

import json
from datetime import datetime

from mock import Mock

from memex_logging.common.dao.common import EntryNotFound
from memex_logging.common.model.message import Message
from test.unit.memex_logging.common_test.common_test_ws import CommonWsTestCase


class TestMessageInterface(CommonWsTestCase):

    def test_get_message(self):
        project = "project"
        message_id = "message_id"
        user_id = "user_id"
        trace_id = "trace_id"

        message = Message.from_repr({
            "messageId": "message_id",
            "conversationId": None,
            "channel": "channel",
            "userId": "user_id",
            "timestamp": "2021-01-22T17:55:33.429203",
            "content": {
                "type": "action",
                "value": "test"
            },
            "domain": None,
            "intent": None,
            "entities": [],
            "language": None,
            "metadata": {},
            "project": "project",
            "type": "request"
        })

        self.dao_collector.message_dao.get_message = Mock(return_value=(message, trace_id))
        response = self.client.get(f"/message?project={project}&messageId={message_id}&userId={user_id}")
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            "messageId": "message_id",
            "conversationId": None,
            "channel": "channel",
            "userId": "user_id",
            "timestamp": "2021-01-22T17:55:33.429203",
            "content": {
                "type": "action",
                "value": "test"
            },
            "domain": None,
            "intent": None,
            "entities": [],
            "language": None,
            "metadata": {},
            "project": "project",
            "type": "request",
            "traceId": trace_id
        }, json.loads(response.data))

        response = self.client.get(f"/message?traceId={trace_id}")
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            "messageId": "message_id",
            "conversationId": None,
            "channel": "channel",
            "userId": "user_id",
            "timestamp": "2021-01-22T17:55:33.429203",
            "content": {
                "type": "action",
                "value": "test"
            },
            "domain": None,
            "intent": None,
            "entities": [],
            "language": None,
            "metadata": {},
            "project": "project",
            "type": "request",
            "traceId": trace_id
        }, json.loads(response.data))

        self.dao_collector.message_dao.get_message = Mock(side_effect=ValueError)
        response = self.client.get("/message")
        self.assertEqual(400, response.status_code)

        self.dao_collector.message_dao.get_message = Mock(side_effect=EntryNotFound)
        response = self.client.get(f"/message?traceId={trace_id}")
        self.assertEqual(404, response.status_code)

        self.dao_collector.message_dao.get_message = Mock(side_effect=Exception)
        response = self.client.get(f"/message?traceId={trace_id}")
        self.assertEqual(500, response.status_code)

    def test_delete_message(self):
        project = "project"
        message_id = "message_id"
        user_id = "user_id"
        trace_id = "trace_id"

        self.dao_collector.message_dao.delete_message = Mock(return_value=None)
        response = self.client.delete(f"/message?project={project}&messageId={message_id}&userId={user_id}")
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            "status": "Ok: message deleted",
            "code": 200
        }, json.loads(response.data))

        response = self.client.delete(f"/message?traceId={trace_id}")
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            "status": "Ok: message deleted",
            "code": 200
        }, json.loads(response.data))

        self.dao_collector.message_dao.delete_message = Mock(side_effect=ValueError)
        response = self.client.delete("/message")
        self.assertEqual(400, response.status_code)

        self.dao_collector.message_dao.delete_message = Mock(side_effect=Exception)
        response = self.client.delete(f"/message?traceId={trace_id}")
        self.assertEqual(500, response.status_code)


class TestMessagesInterface(CommonWsTestCase):

    def test_post_messages(self):
        raw_messages = [{
            "messageId": "message_id",
            "conversationId": None,
            "channel": "channel",
            "userId": "user_id",
            "timestamp": "2021-01-22T17:55:33.429203",
            "content": {
                "type": "action",
                "value": "test"
            },
            "domain": None,
            "intent": None,
            "entities": [],
            "language": None,
            "metadata": {},
            "project": "project",
            "type": "request"
        }]

        self.dao_collector.message_dao.add_messages = Mock(return_value="trace_id")
        response = self.client.post("/messages", json=raw_messages)
        self.assertEqual(201, response.status_code)
        self.assertEqual({
            "traceIds": ["trace_id"],
            "status": "Created: messages stored",
            "code": 201
        }, json.loads(response.data))

        response = self.client.post("/messages")
        self.assertEqual(400, response.status_code)

        self.dao_collector.message_dao.add_messages = Mock(side_effect=Exception)
        response = self.client.post("/messages", json=raw_messages)
        self.assertEqual(500, response.status_code)

        raw_messages = [{
            "messageId": "message_id"
        }]
        response = self.client.post("/messages", json=raw_messages)
        self.assertEqual(400, response.status_code)

    def test_get_messages(self):
        project = "project"
        from_time = datetime(2021, 2, 4).isoformat()
        to_time = datetime(2021, 2, 5).isoformat()

        messages = [Message.from_repr({
            "messageId": "message_id",
            "conversationId": None,
            "channel": "channel",
            "userId": "user_id",
            "timestamp": "2021-01-22T17:55:33.429203",
            "content": {
                "type": "action",
                "value": "test"
            },
            "domain": None,
            "intent": None,
            "entities": [],
            "language": None,
            "metadata": {},
            "project": "project",
            "type": "request"
        })]

        self.dao_collector.message_dao.search_messages = Mock(return_value=messages)
        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}")
        self.assertEqual(200, response.status_code)
        self.assertEqual([message.to_repr() for message in messages], json.loads(response.data))

        response = self.client.get(f"/messages?project={project}&fromTime={from_time}")
        self.assertEqual(400, response.status_code)

        response = self.client.get(f"/messages?project={project}&toTime={to_time}")
        self.assertEqual(400, response.status_code)

        response = self.client.get(f"/messages?fromTime={from_time}&toTime={to_time}")
        self.assertEqual(400, response.status_code)

        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}&userId=user_id")
        self.assertEqual(200, response.status_code)

        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}&channel=channel")
        self.assertEqual(200, response.status_code)

        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}&type=request")
        self.assertEqual(200, response.status_code)

        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}&maxSize=1000")
        self.assertEqual(200, response.status_code)

        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}&maxSize=10001")
        self.assertEqual(400, response.status_code)

        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}&maxSize=30.5")
        self.assertEqual(400, response.status_code)

        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}&maxSize=cinque")
        self.assertEqual(400, response.status_code)

        self.dao_collector.message_dao.search_messages = Mock(side_effect=Exception)
        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}")
        self.assertEqual(500, response.status_code)

        from_time = datetime(2021, 2, 6).isoformat()
        to_time = datetime(2021, 2, 5).isoformat()
        self.dao_collector.message_dao.search_messages = Mock(side_effect=ValueError)
        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}")
        self.assertEqual(400, response.status_code)

        from_time = "from_time"
        to_time = "to_time"
        response = self.client.get(f"/messages?project={project}&fromTime={from_time}&toTime={to_time}")
        self.assertEqual(400, response.status_code)
