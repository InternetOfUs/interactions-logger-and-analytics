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

from memex_logging.common.model.result import AnalyticResult, ConversationLengthAnalyticResult, ConversationLength, \
    ConversationPathAnalyticResult, ConversationPath, SegmentationAnalyticResult, Segmentation, \
    TransactionAnalyticResult, TransactionReturn, AggregationResult


class TestAnalyticResult(TestCase):

    def test_repr(self):
        result = AnalyticResult(1, ["1"], "userId", datetime.now(), datetime.now(), datetime.now())
        self.assertEqual(result, AnalyticResult.from_repr(result.to_repr()))


class TestConversationLength(TestCase):

    def test_repr(self):
        length = ConversationLength("1", 2)
        self.assertEqual(length, ConversationLength.from_repr(length.to_repr()))


class TestConversationLengthAnalyticResult(TestCase):

    def test_repr(self):
        response = ConversationLengthAnalyticResult(1, [ConversationLength("1", 2)], datetime.now(), datetime.now(), datetime.now())
        self.assertEqual(response, ConversationLengthAnalyticResult.from_repr(response.to_repr()))


class TestConversationPath(TestCase):

    def test_repr(self):
        path = ConversationPath("1", ["1"])
        self.assertEqual(path, ConversationPath.from_repr(path.to_repr()))


class TestConversationPathAnalyticResult(TestCase):

    def test_repr(self):
        response = ConversationPathAnalyticResult(1, [ConversationPath("1", ["1"])], datetime.now(), datetime.now(), datetime.now())
        self.assertEqual(response, ConversationPathAnalyticResult.from_repr(response.to_repr()))


class TestSegmentation(TestCase):

    def test_repr(self):
        segmentation = Segmentation("type1", 1)
        self.assertEqual(segmentation, Segmentation.from_repr(segmentation.to_repr()))


class TestSegmentationAnalyticResult(TestCase):

    def test_repr(self):
        response = SegmentationAnalyticResult([Segmentation("type1", 1)], datetime.now(), datetime.now(), datetime.now())
        self.assertEqual(response, SegmentationAnalyticResult.from_repr(response.to_repr()))


class TestTransactionReturn(TestCase):

    def test_repr(self):
        transaction_return = TransactionReturn("task_id", ["1"])
        self.assertEqual(transaction_return, TransactionReturn.from_repr(transaction_return.to_repr()))


class TestTransactionAnalyticResult(TestCase):

    def test_repr(self):
        response = TransactionAnalyticResult(1, [TransactionReturn("task_id", ["1"])], datetime.now(), datetime.now(), datetime.now())
        self.assertEqual(response, TransactionAnalyticResult.from_repr(response.to_repr()))


class TestAggregationResult(TestCase):

    def test_repr(self):
        response = AggregationResult("min", 0.1, datetime.now(), datetime.now(), datetime.now())
        self.assertEqual(response, AggregationResult.from_repr(response.to_repr(), response.aggregation))
