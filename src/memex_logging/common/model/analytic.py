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

from abc import ABC, abstractmethod
from typing import Union

from memex_logging.common.model.time import MovingTimeWindow, FixedTimeWindow


class CommonAnalytic(ABC):

    def __init__(self, timespan: Union[MovingTimeWindow, FixedTimeWindow], project: str) -> None:
        self.timespan = timespan
        self.project = project

    @abstractmethod
    def to_repr(self) -> dict:
        pass


class DimensionAnalytic(CommonAnalytic):

    ANALYTIC_TYPE = "analytic"

    def __init__(self, timespan: Union[MovingTimeWindow, FixedTimeWindow], project: str, dimension: str, metric: str) -> None:
        super().__init__(timespan, project)
        self.dimension = dimension
        self.metric = metric

    def to_repr(self) -> dict:
        return {
            'timespan': self.timespan.to_repr(),
            'project': self.project,
            'type': self.ANALYTIC_TYPE,
            'dimension': self.dimension,
            'metric': self.metric
        }

    @staticmethod
    def from_repr(raw_data: dict) -> DimensionAnalytic:
        if str(raw_data['type']).lower() != DimensionAnalytic.ANALYTIC_TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for Analytic")

        if str(raw_data['timespan']['type']).upper() not in [MovingTimeWindow.moving_time_window_type(),
                                                             MovingTimeWindow.default_time_window_type(),
                                                             FixedTimeWindow.fixed_time_window_type(),
                                                             FixedTimeWindow.custom_time_window_type()]:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() == UserAnalytic.USER_DIMENSION:
            return UserAnalytic.from_repr(raw_data)
        elif str(raw_data['dimension']).lower() == MessageAnalytic.MESSAGE_DIMENSION:
            return MessageAnalytic.from_repr(raw_data)
        elif str(raw_data['dimension']).lower() == TaskAnalytic.TASK_DIMENSION:
            return TaskAnalytic.from_repr(raw_data)
        elif str(raw_data['dimension']).lower() == TransactionAnalytic.TRANSACTION_DIMENSION:
            return TransactionAnalytic.from_repr(raw_data)
        elif str(raw_data['dimension']).lower() == ConversationAnalytic.CONVERSATION_DIMENSION:
            return ConversationAnalytic.from_repr(raw_data)
        elif str(raw_data['dimension']).lower() == DialogueAnalytic.DIALOGUE_DIMENSION:
            return DialogueAnalytic.from_repr(raw_data)
        elif str(raw_data['dimension']).lower() == BotAnalytic.BOT_DIMENSION:
            return BotAnalytic.from_repr(raw_data)
        else:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for Analytic")

    def __eq__(self, o) -> bool:
        if isinstance(o, DimensionAnalytic):
            return o.timespan == self.timespan and o.project == self.project and o.dimension == self.dimension and o.metric == self.metric
        else:
            return False


class UserAnalytic(DimensionAnalytic):

    USER_DIMENSION = "user"
    ALLOWED_USER_METRIC_VALUES = ["u:total", "u:active", "u:engaged", "u:new", "a:segmentation", "g:segmentation"]

    def __init__(self, timespan: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(timespan, project, self.USER_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> UserAnalytic:
        if str(raw_data['type']).lower() != UserAnalytic.ANALYTIC_TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for UserAnalytic")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.moving_time_window_type(), MovingTimeWindow.default_time_window_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.fixed_time_window_type(), FixedTimeWindow.custom_time_window_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != UserAnalytic.USER_DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for UserAnalytic")

        if str(raw_data['metric']).lower() not in UserAnalytic.ALLOWED_USER_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for UserAnalytic")

        return UserAnalytic(timespan, raw_data['project'], raw_data['metric'])


class MessageAnalytic(DimensionAnalytic):

    MESSAGE_DIMENSION = "message"
    ALLOWED_MESSAGE_METRIC_VALUES = ["m:from_users", "m:from_bot", "m:responses", "m:notifications", "m:unhandled", "m:segmentation", "u:segmentation"]

    def __init__(self, timespan: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(timespan, project, self.MESSAGE_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> MessageAnalytic:
        if str(raw_data['type']).lower() != MessageAnalytic.ANALYTIC_TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for MessageAnalytic")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.moving_time_window_type(), MovingTimeWindow.default_time_window_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.fixed_time_window_type(), FixedTimeWindow.custom_time_window_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != MessageAnalytic.MESSAGE_DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for MessageAnalytic")

        if str(raw_data['metric']).lower() not in MessageAnalytic.ALLOWED_MESSAGE_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for MessageAnalytic")

        return MessageAnalytic(timespan, raw_data['project'], raw_data['metric'])


class TaskAnalytic(DimensionAnalytic):

    TASK_DIMENSION = "task"
    ALLOWED_TASK_METRIC_VALUES = ["t:total", "t:active", "t:closed", "t:new"]

    def __init__(self, timespan: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(timespan, project, self.TASK_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> TaskAnalytic:
        if str(raw_data['type']).lower() != TaskAnalytic.ANALYTIC_TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for TaskAnalytic")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.moving_time_window_type(), MovingTimeWindow.default_time_window_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.fixed_time_window_type(), FixedTimeWindow.custom_time_window_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != TaskAnalytic.TASK_DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for TaskAnalytic")

        if str(raw_data['metric']).lower() not in TaskAnalytic.ALLOWED_TASK_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for TaskAnalytic")

        return TaskAnalytic(timespan, raw_data['project'], raw_data['metric'])


class TransactionAnalytic(DimensionAnalytic):

    TRANSACTION_DIMENSION = "transaction"
    ALLOWED_TRANSACTION_METRIC_VALUES = ["t:total", "t:segmentation"]

    def __init__(self, timespan: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str, task_id: str = None):
        super().__init__(timespan, project, self.TRANSACTION_DIMENSION, metric)
        self.task_id = task_id

    def to_repr(self) -> dict:
        representation = super().to_repr()
        representation['taskId'] = self.task_id
        return representation

    @staticmethod
    def from_repr(raw_data: dict) -> TransactionAnalytic:
        if str(raw_data['type']).lower() != TransactionAnalytic.ANALYTIC_TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for TransactionAnalytic")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.moving_time_window_type(), MovingTimeWindow.default_time_window_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.fixed_time_window_type(), FixedTimeWindow.custom_time_window_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != TransactionAnalytic.TRANSACTION_DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for TransactionAnalytic")

        if str(raw_data['metric']).lower() not in TransactionAnalytic.ALLOWED_TRANSACTION_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for TransactionAnalytic")

        return TransactionAnalytic(timespan, raw_data['project'], raw_data['metric'], task_id=raw_data.get('taskId'))


class ConversationAnalytic(DimensionAnalytic):

    CONVERSATION_DIMENSION = "conversation"
    ALLOWED_CONVERSATION_METRIC_VALUES = ["c:total", "c:new", "c:length", "c:path"]

    def __init__(self, timespan: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(timespan, project, self.CONVERSATION_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationAnalytic:
        if str(raw_data['type']).lower() != ConversationAnalytic.ANALYTIC_TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for ConversationAnalytic")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.moving_time_window_type(), MovingTimeWindow.default_time_window_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.fixed_time_window_type(), FixedTimeWindow.custom_time_window_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != ConversationAnalytic.CONVERSATION_DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for ConversationAnalytic")

        if str(raw_data['metric']).lower() not in ConversationAnalytic.ALLOWED_CONVERSATION_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for ConversationAnalytic")

        return ConversationAnalytic(timespan, raw_data['project'], raw_data['metric'])


class DialogueAnalytic(DimensionAnalytic):

    DIALOGUE_DIMENSION = "dialogue"
    ALLOWED_DIALOGUE_METRIC_VALUES = ["d:fallback", "d:intents", "d:domains"]

    def __init__(self, timespan: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(timespan, project, self.DIALOGUE_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> DialogueAnalytic:
        if str(raw_data['type']).lower() != DialogueAnalytic.ANALYTIC_TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for DialogueAnalytic")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.moving_time_window_type(), MovingTimeWindow.default_time_window_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.fixed_time_window_type(), FixedTimeWindow.custom_time_window_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != DialogueAnalytic.DIALOGUE_DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for DialogueAnalytic")

        if str(raw_data['metric']).lower() not in DialogueAnalytic.ALLOWED_DIALOGUE_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for DialogueAnalytic")

        return DialogueAnalytic(timespan, raw_data['project'], raw_data['metric'])


class BotAnalytic(DimensionAnalytic):

    BOT_DIMENSION = "bot"
    ALLOWED_BOT_METRIC_VALUES = ["b:response"]

    def __init__(self, timespan: Union[MovingTimeWindow, FixedTimeWindow], project: str, metric: str):
        super().__init__(timespan, project, self.BOT_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> BotAnalytic:
        if str(raw_data['type']).lower() != BotAnalytic.ANALYTIC_TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for BotAnalytic")

        if str(raw_data['timespan']['type']).upper() in [MovingTimeWindow.moving_time_window_type(), MovingTimeWindow.default_time_window_type()]:
            timespan = MovingTimeWindow.from_repr(raw_data['timespan'])
        elif str(raw_data['timespan']['type']).upper() in [FixedTimeWindow.fixed_time_window_type(), FixedTimeWindow.custom_time_window_type()]:
            timespan = FixedTimeWindow.from_repr(raw_data['timespan'])
        else:
            raise ValueError(f"Unrecognized type [{raw_data['timespan']['type']}] for timespan")

        if str(raw_data['dimension']).lower() != BotAnalytic.BOT_DIMENSION:
            raise ValueError(f"Unrecognized dimension [{raw_data['dimension']}] for BotAnalytic")

        if str(raw_data['metric']).lower() not in BotAnalytic.ALLOWED_BOT_METRIC_VALUES:
            raise ValueError(f"Unknown value for metric [{raw_data['metric']}] for BotAnalytic")

        return BotAnalytic(timespan, raw_data['project'], raw_data['metric'])
