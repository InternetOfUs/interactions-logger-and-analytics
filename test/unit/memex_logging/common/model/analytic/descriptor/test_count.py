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

from memex_logging.common.model.analytic.descriptor.builder import AnalyticDescriptorBuilder
from memex_logging.common.model.analytic.descriptor.count import UserCountDescriptor, CountDescriptor, MessageCountDescriptor, TaskCountDescriptor, \
    TransactionCountDescriptor, ConversationCountDescriptor, DialogueCountDescriptor, BotCountDescriptor
from test.unit.memex_logging.common_test.generator.time import TimeGenerator


class TestUserCountDescriptor(TestCase):

    def test_repr(self):
        descriptor = UserCountDescriptor(TimeGenerator.generate_random(), "project", "total")
        self.assertEqual(descriptor, UserCountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, CountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))


class TestMessageCountDescriptor(TestCase):

    def test_repr(self):
        descriptor = MessageCountDescriptor(TimeGenerator.generate_random(), "project", "from_users")
        self.assertEqual(descriptor, MessageCountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, CountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))


class TestTaskCountDescriptor(TestCase):

    def test_repr(self):
        descriptor = TaskCountDescriptor(TimeGenerator.generate_random(), "project", "active")
        self.assertEqual(descriptor, TaskCountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, CountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))


class TestTransactionCountDescriptor(TestCase):

    def test_repr(self):
        descriptor = TransactionCountDescriptor(TimeGenerator.generate_random(), "project", "total")
        self.assertEqual(descriptor, TransactionCountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, CountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))


class TestConversationCountDescriptor(TestCase):

    def test_repr(self):
        descriptor = ConversationCountDescriptor(TimeGenerator.generate_random(), "project", "new")
        self.assertEqual(descriptor, ConversationCountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, CountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))


class TestDialogueCountDescriptor(TestCase):

    def test_repr(self):
        descriptor = DialogueCountDescriptor(TimeGenerator.generate_random(), "project", "intents")
        self.assertEqual(descriptor, DialogueCountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, CountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))


class TestBotCountDescriptor(TestCase):

    def test_repr(self):
        descriptor = BotCountDescriptor(TimeGenerator.generate_random(), "project", "response")
        self.assertEqual(descriptor, BotCountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, CountDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))
