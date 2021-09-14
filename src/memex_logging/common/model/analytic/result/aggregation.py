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

from memex_logging.common.model.analytic.result.common import CommonAnalyticResult


class AggregationResult(CommonAnalyticResult):

    TYPE = "aggregation"

    def __init__(self, aggregation_result: dict,
                 creation_datetime: datetime, from_datetime: datetime, to_datetime: datetime) -> None:
        super().__init__(creation_datetime, from_datetime, to_datetime)
        self.aggregation_result = aggregation_result

    def to_repr(self) -> dict:
        return {
            'aggregation': self.aggregation_result,
            'type': self.TYPE,
            'creationDt': self.creation_datetime.isoformat(),
            'fromDt': self.from_datetime.isoformat(),
            'toDt': self.to_datetime.isoformat()
        }

    @staticmethod
    def from_repr(raw_data: dict) -> AggregationResult:
        if raw_data['type'].lower() != AggregationResult.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for AggregationResult")

        return AggregationResult(
            raw_data['aggregation'],
            datetime.fromisoformat(raw_data['creationDt']),
            datetime.fromisoformat(raw_data['fromDt']),
            datetime.fromisoformat(raw_data['toDt'])
        )

    def __eq__(self, o) -> bool:
        if isinstance(o, AggregationResult):
            return o.aggregation_result == self.aggregation_result and o.creation_datetime == self.creation_datetime \
                   and o.from_datetime == self.from_datetime and o.to_datetime == self.to_datetime
        else:
            return False
