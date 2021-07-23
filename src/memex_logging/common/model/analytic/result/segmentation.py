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
from typing import List

from memex_logging.common.model.analytic.result.common import CommonAnalyticResult


class Segmentation:

    def __init__(self, segmentation_type: str, count: int) -> None:
        self.segmentation_type = segmentation_type
        self.count = count

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'type': self.segmentation_type
        }

    @staticmethod
    def from_repr(raw_data: dict) -> Segmentation:
        return Segmentation(raw_data['type'], raw_data['count'])

    def __eq__(self, o) -> bool:
        if isinstance(o, Segmentation):
            return o.segmentation_type == self.segmentation_type and o.count == self.count
        else:
            return False


class SegmentationResult(CommonAnalyticResult):

    TYPE = "segmentation"

    def __init__(self, segments: List[Segmentation], creation_datetime: datetime,
                 from_datetime: datetime, to_datetime: datetime) -> None:
        super().__init__(creation_datetime, from_datetime, to_datetime)
        self.segments = segments

    def to_repr(self) -> dict:
        return {
            'segments': [count.to_repr() for count in self.segments],
            'type': self.TYPE,
            'creationDt': self.creation_datetime.isoformat(),
            'fromDt': self.from_datetime.isoformat(),
            'toDt': self.to_datetime.isoformat()
        }

    @staticmethod
    def from_repr(raw_data: dict) -> SegmentationResult:
        if raw_data['type'].lower() != SegmentationResult.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for SegmentationAnalyticResult")

        counts = [Segmentation.from_repr(count) for count in raw_data['segments']]
        return SegmentationResult(
            counts,
            datetime.fromisoformat(raw_data['creationDt']),
            datetime.fromisoformat(raw_data['fromDt']),
            datetime.fromisoformat(raw_data['toDt'])
        )

    def __eq__(self, o) -> bool:
        if isinstance(o, SegmentationResult):
            return o.segments == self.segments and o.creation_datetime == self.creation_datetime \
                   and o.from_datetime == self.from_datetime and o.to_datetime == self.to_datetime
        else:
            return False
