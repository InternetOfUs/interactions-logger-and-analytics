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

from typing import Union

from memex_logging.common.model.analytic.descriptor.common import CommonAnalyticDescriptor
from memex_logging.common.model.time import MovingTimeWindow, FixedTimeWindow, TimeWindow


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
        raw_data['type'] = raw_data['type'].lower
        if raw_data['type'] != CountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for CountDescriptor")

        raw_data['dimension'] = raw_data['dimension'].lower
        dimension = raw_data['dimension']
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
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for CountDescriptor")

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
        descriptor_type = raw_data['type'].lower()
        if descriptor_type != UserCountDescriptor.TYPE:
            raise ValueError(f"Unsupported type [{raw_data['type']}] for UserCountDescriptor")

        time_span = TimeWindow.from_repr(raw_data['timespan'])

        descriptor_dimension = raw_data['dimension'].lower()
        if descriptor_dimension != UserCountDescriptor.DIMENSION:
            raise ValueError(f"Unsupported dimension [{descriptor_dimension}] for UserCountDescriptor")

        descriptor_metric = raw_data["metric"].lower()
        if descriptor_metric not in UserCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{descriptor_metric}] for UserCountDescriptor")

        return UserCountDescriptor(time_span, raw_data['project'], raw_data['metric'])


class MessageCountDescriptor(CountDescriptor):

    DIMENSION = "message"
    ALLOWED_METRIC_VALUES = ["from_users", "from_bot", "responses", "notifications"]  # "unhandled"

    def __init__(self, time_span: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> MessageCountDescriptor:
        if str(raw_data['type']).lower() != MessageCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for MessageCountDescriptor")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.type(), MovingTimeWindow.deprecated_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.type(), FixedTimeWindow.deprecated_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != MessageCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for MessageCountDescriptor")

        if str(raw_data['metric']).lower() not in MessageCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for MessageCountDescriptor")

        return MessageCountDescriptor(timespan, raw_data['project'], raw_data['metric'])


class TaskCountDescriptor(CountDescriptor):

    DIMENSION = "task"
    ALLOWED_METRIC_VALUES = ["total", "active", "closed", "new"]

    def __init__(self, time_span: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> TaskCountDescriptor:
        if str(raw_data['type']).lower() != TaskCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for TaskCountDescriptor")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.type(), MovingTimeWindow.deprecated_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.type(), FixedTimeWindow.deprecated_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != TaskCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for TaskCountDescriptor")

        if str(raw_data['metric']).lower() not in TaskCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for TaskCountDescriptor")

        return TaskCountDescriptor(timespan, raw_data['project'], raw_data['metric'])


class TransactionCountDescriptor(CountDescriptor):

    DIMENSION = "transaction"
    ALLOWED_METRIC_VALUES = ["total"]

    def __init__(self, time_span: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str, task_id: str = None):
        super().__init__(time_span, project, self.DIMENSION, metric)
        self.task_id = task_id

    def to_repr(self) -> dict:
        representation = super().to_repr()
        representation['taskId'] = self.task_id
        return representation

    @staticmethod
    def from_repr(raw_data: dict) -> TransactionCountDescriptor:
        if str(raw_data['type']).lower() != TransactionCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for TransactionCountDescriptor")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.type(), MovingTimeWindow.deprecated_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.type(), FixedTimeWindow.deprecated_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != TransactionCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for TransactionCountDescriptor")

        if str(raw_data['metric']).lower() not in TransactionCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for TransactionCountDescriptor")

        return TransactionCountDescriptor(timespan, raw_data['project'], raw_data['metric'], task_id=raw_data.get('taskId'))


class ConversationCountDescriptor(CountDescriptor):

    DIMENSION = "conversation"
    ALLOWED_METRIC_VALUES = ["total", "new"]  # "length", "path"

    def __init__(self, time_span: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationCountDescriptor:
        if str(raw_data['type']).lower() != ConversationCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for ConversationCountDescriptor")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.type(), MovingTimeWindow.deprecated_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.type(), FixedTimeWindow.deprecated_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != ConversationCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for ConversationCountDescriptor")

        if str(raw_data['metric']).lower() not in ConversationCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for ConversationCountDescriptor")

        return ConversationCountDescriptor(timespan, raw_data['project'], raw_data['metric'])


class DialogueCountDescriptor(CountDescriptor):

    DIMENSION = "dialogue"
    ALLOWED_METRIC_VALUES = ["fallback", "intents", "domains"]

    def __init__(self, time_span: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> DialogueCountDescriptor:
        if str(raw_data['type']).lower() != DialogueCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for DialogueCountDescriptor")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.type(), MovingTimeWindow.deprecated_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.type(), FixedTimeWindow.deprecated_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != DialogueCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for DialogueCountDescriptor")

        if str(raw_data['metric']).lower() not in DialogueCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for DialogueCountDescriptor")

        return DialogueCountDescriptor(timespan, raw_data['project'], raw_data['metric'])


class BotCountDescriptor(CountDescriptor):

    DIMENSION = "bot"
    ALLOWED_METRIC_VALUES = ["response"]

    def __init__(self, time_span: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(time_span, project, self.DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> BotCountDescriptor:
        if str(raw_data['type']).lower() != BotCountDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for BotCountDescriptor")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.type(), MovingTimeWindow.deprecated_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.type(), FixedTimeWindow.deprecated_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != BotCountDescriptor.DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for BotCountDescriptor")

        if str(raw_data['metric']).lower() not in BotCountDescriptor.ALLOWED_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for BotCountDescriptor")

        return BotCountDescriptor(timespan, raw_data['project'], raw_data['metric'])
