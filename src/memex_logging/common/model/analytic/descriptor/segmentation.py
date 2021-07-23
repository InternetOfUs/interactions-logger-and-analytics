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

from memex_logging.common.model.analytic.descriptor.common import CommonAnalyticDescriptor
from memex_logging.common.model.time import TimeWindow


class SegmentationDescriptor(CommonAnalyticDescriptor):

    TYPE = "segmentation"

    def __init__(self, timespan: TimeWindow, project: str, dimension: str, metric: str) -> None:
        super().__init__(timespan, project)
        self.dimension = dimension
        self.metric = metric

    def to_repr(self) -> dict:
        return {
            'timespan': self.timespan.to_repr(),
            'project': self.project,
            'type': self.TYPE,
            'dimension': self.dimension,
            'metric': self.metric
        }

    @staticmethod
    def from_repr(raw_data: dict) -> SegmentationDescriptor:
        if raw_data['type'].lower() != SegmentationDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for SegmentationDescriptor")

        dimension = raw_data['dimension'].lower()
        if dimension == UserSegmentationDescriptor.DIMENSION:
            return UserSegmentationDescriptor.from_repr(raw_data)
        elif dimension == MessageSegmentationDescriptor.DIMENSION:
            return MessageSegmentationDescriptor.from_repr(raw_data)
        elif dimension == TransactionSegmentationDescriptor.DIMENSION:
            return TransactionSegmentationDescriptor.from_repr(raw_data)
        else:
            raise ValueError(f"Unrecognized dimension [{dimension}] for SegmentationDescriptor")

    def __eq__(self, o) -> bool:
        if isinstance(o, SegmentationDescriptor):
            return o.timespan == self.timespan and o.project == self.project and o.dimension == self.dimension and o.metric == self.metric
        else:
            return False


class UserSegmentationDescriptor(SegmentationDescriptor):

    DIMENSION = "user"
    ALLOWED_METRIC_VALUES = ["age", "gender"]

    def __init__(self, timespan: TimeWindow, project: str, metric: str):
        super().__init__(timespan, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> UserSegmentationDescriptor:
        if raw_data['type'].lower() != UserSegmentationDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for UserSegmentationDescriptor")

        if raw_data['dimension'].lower() != UserSegmentationDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for UserSegmentationDescriptor")

        timespan = TimeWindow.from_repr(raw_data['timespan'])

        metric = raw_data['metric'].lower()
        if metric not in UserSegmentationDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{metric}] for UserSegmentationDescriptor")

        return UserSegmentationDescriptor(timespan, raw_data['project'], metric)


class MessageSegmentationDescriptor(SegmentationDescriptor):

    DIMENSION = "message"
    ALLOWED_METRIC_VALUES = ["all", "from_users"]

    def __init__(self, timespan: TimeWindow, project: str, metric: str):
        super().__init__(timespan, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> MessageSegmentationDescriptor:
        if raw_data['type'].lower() != MessageSegmentationDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for MessageSegmentationDescriptor")

        if raw_data['dimension'].lower() != MessageSegmentationDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for MessageSegmentationDescriptor")

        timespan = TimeWindow.from_repr(raw_data['timespan'])

        metric = raw_data['metric'].lower()
        if metric not in MessageSegmentationDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{metric}] for MessageSegmentationDescriptor")

        return MessageSegmentationDescriptor(timespan, raw_data['project'], metric)


class TransactionSegmentationDescriptor(SegmentationDescriptor):

    DIMENSION = "transaction"
    ALLOWED_METRIC_VALUES = ["label"]

    def __init__(self, timespan: TimeWindow, project: str, metric: str, task_id: str = None):
        super().__init__(timespan, project, self.DIMENSION, metric)
        self.task_id = task_id

    def to_repr(self) -> dict:
        representation = super().to_repr()
        representation['taskId'] = self.task_id
        return representation

    @staticmethod
    def from_repr(raw_data: dict) -> TransactionSegmentationDescriptor:
        if raw_data['type'].lower() != TransactionSegmentationDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for TransactionSegmentationDescriptor")

        if raw_data['dimension'].lower() != TransactionSegmentationDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for TransactionSegmentationDescriptor")

        timespan = TimeWindow.from_repr(raw_data['timespan'])

        metric = raw_data['metric'].lower()
        if metric not in TransactionSegmentationDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{metric}] for TransactionSegmentationDescriptor")

        return TransactionSegmentationDescriptor(timespan, raw_data['project'], metric, task_id=raw_data.get('taskId'))
