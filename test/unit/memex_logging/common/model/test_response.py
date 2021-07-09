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

from memex_logging.common.model.aggregation import AggregationAnalytic
from memex_logging.common.model.analytic import UserAnalytic
from memex_logging.common.model.response import AnalyticResponse, AggregationResponse
from memex_logging.common.model.result import AnalyticResult, AggregationResult
from memex_logging.common.model.time import MovingTimeWindow


class TestAnalyticResponse(TestCase):

    def test_repr(self):
        response = AnalyticResponse(UserAnalytic(MovingTimeWindow("7D"), "project", "u:total"), AnalyticResult(1, ["1"], "userId", datetime.now(), datetime.now(), datetime.now()), "static_id")
        self.assertEqual(response, AnalyticResponse.from_repr(response.to_repr()))


class TestAggregationResponse(TestCase):

    def test_repr(self):
        response = AggregationResponse(AggregationAnalytic(MovingTimeWindow("7D"), "project", "intent.confidence", "avg", None), AggregationResult("avg", 0.78, datetime.now(), datetime.now(), datetime.now()), "static_id")
        self.assertEqual(response, AggregationResponse.from_repr(response.to_repr()))
