from __future__ import absolute_import, annotations

import json
from unittest import TestCase

from memex_logging.models.message import RequestMessage, Intent, ActionRequest


class TestIntent(TestCase):

    def test_repr(self):
        intent_no_confidence = Intent("intent", None)
        intent_repr = intent_no_confidence.to_repr()
        intent_from_repr = Intent.from_rep(intent_repr)

        self.assertEqual(intent_no_confidence, intent_from_repr)

        intent_with_confidence = Intent("intent", 0.9)
        intent_repr = intent_with_confidence.to_repr()
        intent_from_repr = Intent.from_rep(intent_repr)

        self.assertEqual(intent_with_confidence, intent_from_repr)

    def test_from_json(self):
        raw_data = {
            "name": "intent",
            "confidence": None
        }
        intent = Intent.from_rep(raw_data)
        self.assertIsInstance(intent, Intent)

        raw_data = {
            "name": "intent",
            "confidence": 0.8
        }
        intent = Intent.from_rep(raw_data)
        self.assertIsInstance(intent, Intent)


class TestActionRequest(TestCase):

    def test_repr(self):
        action = ActionRequest("actionName")
        action_repr = action.to_repr()
        action_from_repr = ActionRequest.from_rep(action_repr)

        self.assertEqual(action, action_from_repr)


class TestMessage(TestCase):

    def test_request(self):
        raw_data = {
            "type": "REQUEST",
            "messageId": "message_id",
            "channel": "channel",
            "userId": "user_id",
            "conversationId": None,
            "project": "project",
            "content": {
                "type": "action",
                "value": "test"
            },
            "domain": None,
            "intent": None,
            "entities": [],
            "language": None,
            "metadata": {},
            "timestamp": "2021-01-22T17:55:33.429203"
          }

        RequestMessage.from_rep(raw_data)
