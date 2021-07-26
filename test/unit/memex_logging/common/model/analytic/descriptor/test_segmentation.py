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
from memex_logging.common.model.analytic.descriptor.segmentation import SegmentationDescriptor, \
    UserSegmentationDescriptor, MessageSegmentationDescriptor, TransactionSegmentationDescriptor
from memex_logging.common.model.analytic.time import MovingTimeWindow


class TestUserSegmentationDescriptor(TestCase):

    def test_repr(self):
        descriptor = UserSegmentationDescriptor(MovingTimeWindow("7D"), "project", "age")
        self.assertEqual(descriptor, UserSegmentationDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, SegmentationDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))


class TestMessageSegmentationDescriptor(TestCase):

    def test_repr(self):
        descriptor = MessageSegmentationDescriptor(MovingTimeWindow("7D"), "project", "all")
        self.assertEqual(descriptor, MessageSegmentationDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, SegmentationDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))


class TestTransactionSegmentationDescriptor(TestCase):

    def test_repr(self):
        descriptor = TransactionSegmentationDescriptor(MovingTimeWindow("7D"), "project", "label")
        self.assertEqual(descriptor, TransactionSegmentationDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, SegmentationDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))
