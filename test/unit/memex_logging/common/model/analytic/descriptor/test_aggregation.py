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
from memex_logging.common.model.analytic.descriptor.aggregation import AggregationDescriptor, Filter
from test.unit.memex_logging.common_test.generator.time import TimeGenerator


class TestAggregationDescriptor(TestCase):

    def test_repr(self):
        descriptor = AggregationDescriptor(TimeGenerator.generate_random(), "project", "intent.confidence", "avg", None)
        self.assertEqual(descriptor, AggregationDescriptor.from_repr(descriptor.to_repr()))
        self.assertEqual(descriptor, AnalyticDescriptorBuilder.build(descriptor.to_repr()))


class TestFilter(TestCase):

    def test_repr(self):
        aggregation_filter = Filter("messageId", "match", "hi")
        self.assertEqual(aggregation_filter, Filter.from_repr(aggregation_filter.to_repr()))
