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

from unittest import TestCase

from memex_logging.common.model.message import Intent, ActionRequest, Message, RequestMessage, ResponseMessage, \
    NotificationMessage, TextualResponse, Entity


class TestIntent(TestCase):

    def test_repr(self):
        intent_no_confidence = Intent("intent", None)
        intent_repr = intent_no_confidence.to_repr()
        intent_from_repr = Intent.from_repr(intent_repr)

        self.assertEqual(intent_no_confidence, intent_from_repr)

        intent_with_confidence = Intent("intent", 0.9)
        intent_repr = intent_with_confidence.to_repr()
        intent_from_repr = Intent.from_repr(intent_repr)

        self.assertEqual(intent_with_confidence, intent_from_repr)

    def test_from_json(self):
        raw_data = {
            "name": "intent",
            "confidence": None
        }
        intent = Intent.from_repr(raw_data)
        self.assertIsInstance(intent, Intent)

        raw_data = {
            "name": "intent",
            "confidence": 0.8
        }
        intent = Intent.from_repr(raw_data)
        self.assertIsInstance(intent, Intent)


class TestActionRequest(TestCase):

    def test_repr(self):
        action = ActionRequest("actionName")
        action_repr = action.to_repr()
        action_from_repr = ActionRequest.from_repr(action_repr)

        self.assertEqual(action, action_from_repr)


class TestTextualResponse(TestCase):

    def test_repr(self):
        text = TextualResponse("actionName", [])
        text_repr = text.to_repr()
        text_from_repr = TextualResponse.from_repr(text_repr)

        self.assertEqual(text, text_from_repr)


class TestEntity(TestCase):

    def test_repr(self):
        entity = Entity("type", "value", 0.0)
        entity_repr = entity.to_repr()
        entity_from_repr = Entity.from_repr(entity_repr)

        self.assertEqual(entity, entity_from_repr)


class TestMessage(TestCase):

    def test_request(self):
        raw_data = {
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
          }

        message = Message.from_repr(raw_data)
        self.assertIsInstance(message, RequestMessage)
        self.assertEqual(raw_data, message.to_repr())

    def test_response(self):
        raw_data = {
            "messageId": "message_id",
            "conversationId": None,
            "channel": "channel",
            "userId": "user_id",
            "responseTo": "responseTo",
            "timestamp": "2021-01-22T17:55:33.429203",
            "content": {
                "type": "text",
                "value": "test",
                "buttons": [],
            },
            "metadata": {},
            "project": "project",
            "type": "response",
        }

        message = Message.from_repr(raw_data)
        self.assertIsInstance(message, ResponseMessage)
        self.assertEqual(raw_data, message.to_repr())

    def test_notification(self):
        raw_data = {
            "messageId": "message_id",
            "conversationId": None,
            "channel": "channel",
            "userId": "user_id",
            "timestamp": "2021-01-22T17:55:33.429203",
            "content": {
                "type": "text",
                "value": "test",
                "buttons": [],
            },
            "metadata": {},
            "project": "project",
            "type": "notification",
        }

        message = Message.from_repr(raw_data)
        self.assertIsInstance(message, NotificationMessage)
        self.assertEqual(raw_data, message.to_repr())
