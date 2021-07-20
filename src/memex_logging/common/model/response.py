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

from typing import Optional

from memex_logging.common.model.aggregation import AggregationAnalytic
from memex_logging.common.model.analytic import DimensionAnalytic
from memex_logging.common.model.result import CommonResult, AggregationResult


class AnalyticResponse:

    def __init__(self, analytic: DimensionAnalytic, result: Optional[CommonResult], static_id: str) -> None:
        self.analytic = analytic
        self.result = result
        self.static_id = static_id

    def to_repr(self) -> dict:
        return {
            'query': self.analytic.to_repr(),
            'result': self.result.to_repr() if self.result is not None else None,
            'staticId': self.static_id
        }

    @staticmethod
    def from_repr(raw_data: dict) -> AnalyticResponse:
        analytic = DimensionAnalytic.from_repr(raw_data['query'])
        result = CommonResult.from_repr(raw_data['result']) if raw_data['result'] is not None else None
        return AnalyticResponse(analytic, result, raw_data['staticId'])

    def __eq__(self, o) -> bool:
        if isinstance(o, AnalyticResponse):
            return o.analytic == self.analytic and o.result == self.result and o.static_id == self.static_id
        else:
            return False


class AggregationResponse:

    def __init__(self, analytic: AggregationAnalytic, result: Optional[AggregationResult], static_id: str) -> None:
        self.analytic = analytic
        self.result = result
        self.static_id = static_id

    def to_repr(self) -> dict:
        return {
            'query': self.analytic.to_repr(),
            'result': self.result.to_repr() if self.result is not None else None,
            'staticId': self.static_id
        }

    @staticmethod
    def from_repr(raw_data: dict) -> AggregationResponse:
        analytic = AggregationAnalytic.from_repr(raw_data['query'])
        result = AggregationResult.from_repr(raw_data['result'], analytic.aggregation) if raw_data['result'] is not None else None
        return AggregationResponse(analytic, result, raw_data['staticId'])

    def __eq__(self, o) -> bool:
        if isinstance(o, AggregationResponse):
            return o.analytic == self.analytic and o.result == self.result and o.static_id == self.static_id
        else:
            return False
