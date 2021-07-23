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

from memex_logging.common.model.analytic.result.aggregation import AggregationResult
from memex_logging.common.model.analytic.result.common import CommonAnalyticResult
from memex_logging.common.model.analytic.result.count import CountResult
from memex_logging.common.model.analytic.result.segmentation import SegmentationResult


class AnalyticResultBuilder:

    @staticmethod
    def build(raw_data: dict) -> CommonAnalyticResult:
        result_type = raw_data['type'].lower()
        if result_type == CountResult.TYPE:
            return CountResult.from_repr(raw_data)
        elif result_type == SegmentationResult.TYPE:
            return SegmentationResult.from_repr(raw_data)
        elif result_type == AggregationResult.TYPE:
            return AggregationResult.from_repr(raw_data)
        else:
            raise ValueError(f"Unrecognized type [{result_type}] for AnalyticResult")
