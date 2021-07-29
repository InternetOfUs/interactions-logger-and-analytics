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


class CountDescriptor(CommonAnalyticDescriptor):

    TYPE = "count"

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
    def from_repr(raw_data: dict) -> CountDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != CountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for CountDescriptor")

        dimension = raw_data['dimension'].lower()
        if dimension == UserCountDescriptor.DIMENSION:
            return UserCountDescriptor.from_repr(raw_data)
        elif dimension == MessageCountDescriptor.DIMENSION:
            return MessageCountDescriptor.from_repr(raw_data)
        elif dimension == TaskCountDescriptor.DIMENSION:
            return TaskCountDescriptor.from_repr(raw_data)
        elif dimension == TransactionCountDescriptor.DIMENSION:
            return TransactionCountDescriptor.from_repr(raw_data)
        elif dimension == ConversationCountDescriptor.DIMENSION:
            return ConversationCountDescriptor.from_repr(raw_data)
        elif dimension == DialogueCountDescriptor.DIMENSION:
            return DialogueCountDescriptor.from_repr(raw_data)
        elif dimension == BotCountDescriptor.DIMENSION:
            return BotCountDescriptor.from_repr(raw_data)
        else:
            raise ValueError(f"Unrecognized dimension [{dimension}] for CountDescriptor")

    def __eq__(self, o) -> bool:
        if isinstance(o, CountDescriptor):
            return o.time_span == self.time_span and o.project == self.project and o.dimension == self.dimension and o.metric == self.metric
        else:
            return False


class UserCountDescriptor(CountDescriptor):

    DIMENSION = "user"
    ALLOWED_METRIC_VALUES = ["total", "active", "engaged", "new"]

    def __init__(self, time_span: TimeWindow, project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> UserCountDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != UserCountDescriptor.TYPE:
            raise ValueError(f"Unsupported type [{analytic_type}] for UserCountDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != UserCountDescriptor.DIMENSION:
            raise ValueError(f"Unsupported dimension [{descriptor_dimension}] for UserCountDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in UserCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for UserCountDescriptor")

        return UserCountDescriptor(time_span, raw_data['project'], descriptor_metric)


class MessageCountDescriptor(CountDescriptor):

    DIMENSION = "message"
    ALLOWED_METRIC_VALUES = ["from_users", "from_bot", "responses", "notifications"]  # "unhandled"

    def __init__(self, time_span: TimeWindow, project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> MessageCountDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != MessageCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for MessageCountDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != MessageCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{descriptor_dimension}] for MessageCountDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in MessageCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for MessageCountDescriptor")

        return MessageCountDescriptor(time_span, raw_data['project'], descriptor_metric)


class TaskCountDescriptor(CountDescriptor):

    DIMENSION = "task"
    ALLOWED_METRIC_VALUES = ["total", "active", "closed", "new"]

    def __init__(self, time_span: TimeWindow, project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> TaskCountDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != TaskCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for TaskCountDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != TaskCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{descriptor_dimension}] for TaskCountDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in TaskCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for TaskCountDescriptor")

        return TaskCountDescriptor(time_span, raw_data['project'], descriptor_metric)


class TransactionCountDescriptor(CountDescriptor):

    DIMENSION = "transaction"
    ALLOWED_METRIC_VALUES = ["total"]

    def __init__(self, time_span: TimeWindow, project: str, metric: str, task_id: str = None):
        super().__init__(time_span, project, self.DIMENSION, metric)
        self.task_id = task_id

    def to_repr(self) -> dict:
        representation = super().to_repr()
        representation['taskId'] = self.task_id
        return representation

    @staticmethod
    def from_repr(raw_data: dict) -> TransactionCountDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != TransactionCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for TransactionCountDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != TransactionCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{descriptor_dimension}] for TransactionCountDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in TransactionCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for TransactionCountDescriptor")

        return TransactionCountDescriptor(time_span, raw_data['project'], descriptor_metric, task_id=raw_data.get('taskId', None))


class ConversationCountDescriptor(CountDescriptor):

    DIMENSION = "conversation"
    ALLOWED_METRIC_VALUES = ["total", "new"]  # "length", "path"

    def __init__(self, time_span: TimeWindow, project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationCountDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != ConversationCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for ConversationCountDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != ConversationCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{descriptor_dimension}] for ConversationCountDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in ConversationCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for ConversationCountDescriptor")

        return ConversationCountDescriptor(time_span, raw_data['project'], descriptor_metric)


class DialogueCountDescriptor(CountDescriptor):

    DIMENSION = "dialogue"
    ALLOWED_METRIC_VALUES = ["fallback", "intents", "domains"]

    def __init__(self, time_span: TimeWindow, project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> DialogueCountDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != DialogueCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for DialogueCountDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != DialogueCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{descriptor_dimension}] for DialogueCountDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in DialogueCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for DialogueCountDescriptor")

        return DialogueCountDescriptor(time_span, raw_data['project'], descriptor_metric)


class BotCountDescriptor(CountDescriptor):

    DIMENSION = "bot"
    ALLOWED_METRIC_VALUES = ["response"]

    def __init__(self, time_span: TimeWindow, project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> BotCountDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != BotCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for BotCountDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != BotCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{descriptor_dimension}] for BotCountDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in BotCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for BotCountDescriptor")

        return BotCountDescriptor(time_span, raw_data['project'], descriptor_metric)
