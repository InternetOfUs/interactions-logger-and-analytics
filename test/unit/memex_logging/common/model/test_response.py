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

from memex_logging.common.model.aggregation import Aggregation
from memex_logging.common.model.analytic import UserAnalytic
from memex_logging.common.model.response import AnalyticResponse, AggregationResponse
from memex_logging.common.model.result import AnalyticResult
from memex_logging.common.model.time import DefaultTime


class TestAnalyticResponse(TestCase):

    def test_repr(self):
        response = AnalyticResponse(UserAnalytic(DefaultTime("7D"), "project", "u:total"), AnalyticResult(1, ["1"], "userId"), "static_id")
        self.assertEqual(response, AnalyticResponse.from_repr(response.to_repr()))


class TestAggregationResponse(TestCase):

    def test_repr(self):
        response = AggregationResponse(Aggregation(DefaultTime("7D"), "project", "intent.confidence", "avg", None), 0.78, "static_id")
        self.assertEqual(response, AggregationResponse.from_repr(response.to_repr()))
