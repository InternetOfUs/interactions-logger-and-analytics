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

from memex_logging.common.analytic.builder import AnalyticBuilder
from memex_logging.common.model.aggregation import AggregationAnalytic, Filter
from memex_logging.common.model.time import MovingTimeWindow


class TestAggregation(TestCase):

    def test_repr(self):
        aggregation = AggregationAnalytic(MovingTimeWindow("7D"), "project", "intent.confidence", "avg", None)
        self.assertEqual(aggregation, AggregationAnalytic.build(aggregation.to_repr()))
        self.assertEqual(aggregation, AnalyticBuilder.build(aggregation.to_repr()))


class TestFilter(TestCase):

    def test_repr(self):
        aggregation_filter = Filter("messageId", "match", "hi")
        self.assertEqual(aggregation_filter, Filter.build(aggregation_filter.to_repr()))
