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
from memex_logging.common.model.analytic.time import TimeWindow


class SegmentationDescriptor(CommonAnalyticDescriptor):

    TYPE = "segmentation"

    def __init__(self, time_span: TimeWindow, project: str, dimension: str, metric: str) -> None:
        super().__init__(time_span, project)
        self.dimension = dimension.lower()
        self.metric = metric.lower()

    def to_repr(self) -> dict:
        return {
            'timespan': self.time_span.to_repr(),
            'project': self.project,
            'type': self.TYPE,
            'dimension': self.dimension,
            'metric': self.metric
        }

    @staticmethod
    def from_repr(raw_data: dict) -> SegmentationDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != SegmentationDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for SegmentationDescriptor")

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
            return o.time_span == self.time_span and o.project == self.project and o.dimension == self.dimension and o.metric == self.metric
        else:
            return False


class UserSegmentationDescriptor(SegmentationDescriptor):

    DIMENSION = "user"
    ALLOWED_METRIC_VALUES = ["age", "gender"]

    def __init__(self, time_span: TimeWindow, project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> UserSegmentationDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != UserSegmentationDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for UserSegmentationDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != UserSegmentationDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{descriptor_dimension}] for UserSegmentationDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in UserSegmentationDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for UserSegmentationDescriptor")

        return UserSegmentationDescriptor(time_span, raw_data['project'], descriptor_metric)


class MessageSegmentationDescriptor(SegmentationDescriptor):

    DIMENSION = "message"
    ALLOWED_METRIC_VALUES = ["all", "requests"]

    def __init__(self, time_span: TimeWindow, project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> MessageSegmentationDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != MessageSegmentationDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for MessageSegmentationDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != MessageSegmentationDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{descriptor_dimension}] for MessageSegmentationDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in MessageSegmentationDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for MessageSegmentationDescriptor")

        return MessageSegmentationDescriptor(time_span, raw_data['project'], descriptor_metric)


class TransactionSegmentationDescriptor(SegmentationDescriptor):

    DIMENSION = "transaction"
    ALLOWED_METRIC_VALUES = ["label"]

    def __init__(self, time_span: TimeWindow, project: str, metric: str, task_id: str = None):
        super().__init__(time_span, project, self.DIMENSION, metric)
        self.task_id = task_id

    def to_repr(self) -> dict:
        representation = super().to_repr()
        representation['taskId'] = self.task_id
        return representation

    @staticmethod
    def from_repr(raw_data: dict) -> TransactionSegmentationDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != TransactionSegmentationDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for TransactionSegmentationDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != TransactionSegmentationDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{descriptor_dimension}] for TransactionSegmentationDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in TransactionSegmentationDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for TransactionSegmentationDescriptor")

        return TransactionSegmentationDescriptor(time_span, raw_data['project'], descriptor_metric, task_id=raw_data.get('taskId'))
