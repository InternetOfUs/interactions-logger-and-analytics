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

from memex_logging.common.model.analytic.descriptor.aggregation import AggregationDescriptor
from memex_logging.common.model.analytic.descriptor.count import UserCountDescriptor
from memex_logging.common.model.analytic.analytic import Analytic
from memex_logging.common.model.analytic.descriptor.segmentation import MessageSegmentationDescriptor
from memex_logging.common.model.analytic.result.aggregation import AggregationResult
from memex_logging.common.model.analytic.result.count import CountResult
from memex_logging.common.model.analytic.result.segmentation import SegmentationResult, Segmentation
from memex_logging.common.model.time import MovingTimeWindow


class TestAnalytic(TestCase):

    def test_repr(self):
        response = Analytic("id", UserCountDescriptor(MovingTimeWindow("7D"), "project", "total"),
                            result=CountResult(1, datetime.now(), datetime.now(), datetime.now()))
        self.assertEqual(response, Analytic.from_repr(response.to_repr()))

        response = Analytic("id", AggregationDescriptor(MovingTimeWindow("7D"), "project", "intent.confidence", "avg", None),
                            result=AggregationResult(0.78, datetime.now(), datetime.now(), datetime.now()))
        self.assertEqual(response, Analytic.from_repr(response.to_repr()))

        response = Analytic("id", MessageSegmentationDescriptor(MovingTimeWindow("7D"), "project", "all"),
                            result=SegmentationResult([Segmentation("request", 1)], datetime.now(), datetime.now(), datetime.now()))
        self.assertEqual(response, Analytic.from_repr(response.to_repr()))

    def test_repr_null_result(self):
        response = Analytic("id", UserCountDescriptor(MovingTimeWindow("7D"), "project", "total"), result=None)
        self.assertEqual(response, Analytic.from_repr(response.to_repr()))

        response = Analytic("id", AggregationDescriptor(MovingTimeWindow("7D"), "project", "intent.confidence", "avg", None), result=None)
        self.assertEqual(response, Analytic.from_repr(response.to_repr()))

        response = Analytic("id", MessageSegmentationDescriptor(MovingTimeWindow("7D"), "project", "all"), result=None)
        self.assertEqual(response, Analytic.from_repr(response.to_repr()))
