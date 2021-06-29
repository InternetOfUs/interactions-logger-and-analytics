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

from datetime import datetime
from unittest import TestCase

from memex_logging.common.analytic.builder import AnalyticBuilder
from memex_logging.common.model.analytic import UserAnalytic, CommonAnalytic, MessageAnalytic, TaskAnalytic, \
    TransactionAnalytic, ConversationAnalytic, DialogueAnalytic, BotAnalytic
from memex_logging.common.model.time import DefaultTime, CustomTime


class TestUserAnalytic(TestCase):

    def test_repr(self):
        analytic = UserAnalytic(DefaultTime("7D"), "project", "u:total")
        self.assertEqual(analytic, UserAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, CommonAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, AnalyticBuilder.from_repr(analytic.to_repr()))


class TestMessageAnalytic(TestCase):

    def test_repr(self):
        analytic = MessageAnalytic(CustomTime(datetime.now(), datetime.now()), "project", "m:segmentation")
        self.assertEqual(analytic, MessageAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, CommonAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, AnalyticBuilder.from_repr(analytic.to_repr()))


class TestTaskAnalytic(TestCase):

    def test_repr(self):
        analytic = TaskAnalytic(DefaultTime("7D"), "project", "t:active")
        self.assertEqual(analytic, TaskAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, CommonAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, AnalyticBuilder.from_repr(analytic.to_repr()))


class TestTransactionAnalytic(TestCase):

    def test_repr(self):
        analytic = TransactionAnalytic(DefaultTime("7D"), "project", "t:segmentation")
        self.assertEqual(analytic, TransactionAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, CommonAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, AnalyticBuilder.from_repr(analytic.to_repr()))


class TestConversationAnalytic(TestCase):

    def test_repr(self):
        analytic = ConversationAnalytic(DefaultTime("7D"), "project", "c:new")
        self.assertEqual(analytic, ConversationAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, CommonAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, AnalyticBuilder.from_repr(analytic.to_repr()))


class TestDialogueAnalytic(TestCase):

    def test_repr(self):
        analytic = DialogueAnalytic(DefaultTime("7D"), "project", "d:fallback")
        self.assertEqual(analytic, DialogueAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, CommonAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, AnalyticBuilder.from_repr(analytic.to_repr()))


class TestBotAnalytic(TestCase):

    def test_repr(self):
        analytic = BotAnalytic(DefaultTime("7D"), "project", "b:response")
        self.assertEqual(analytic, BotAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, CommonAnalytic.from_repr(analytic.to_repr()))
        self.assertEqual(analytic, AnalyticBuilder.from_repr(analytic.to_repr()))
