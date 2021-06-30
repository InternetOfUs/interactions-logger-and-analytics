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

from abc import ABC
from typing import Union

from memex_logging.common.model.time import DefaultTime, CustomTime


class CommonAnalytic(ABC):

    ANALYTIC_TYPE = "analytic"

    def __init__(self, timespan: Union[DefaultTime, CustomTime], project: str, dimension: str, metric: str) -> None:
        self.timespan = timespan
        self.project = project
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
    def from_repr(raw_data: dict) -> CommonAnalytic:
        if 'type' in raw_data:
            if str(raw_data['type']).lower() != CommonAnalytic.ANALYTIC_TYPE:
                raise ValueError("Unrecognized type for Analytic")
        else:
            raise ValueError("Analytic must contain a type")

        if 'project' not in raw_data:
            raise ValueError("Analytic must contain a project")

        if 'timespan' in raw_data:
            if str(raw_data['timespan']['type']).upper() not in [DefaultTime.DEFAULT_TIME_TYPE, CustomTime.CUSTOM_TIME_TYPE]:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("Analytic must contain a timespan")

        if 'metric' not in raw_data:
            raise ValueError('A metric must be defined in the Analytic object')

        if 'dimension' in raw_data:
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
                raise ValueError("Unrecognized dimension for Analytic")
        else:
            raise ValueError("An Analytic must contain a dimension")

    def __eq__(self, o) -> bool:
        if isinstance(o, CommonAnalytic):
            return o.timespan == self.timespan and o.project == self.project and o.dimension == self.dimension and o.metric == self.metric
        else:
            return False


class UserAnalytic(CommonAnalytic):

    USER_DIMENSION = "user"
    ALLOWED_USER_METRIC_VALUES = ["u:total", "u:active", "u:engaged", "u:new"]

    def __init__(self, timespan: Union[DefaultTime, CustomTime], project: str, metric: str):
        super().__init__(timespan, project, self.USER_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> UserAnalytic:
        if 'type' in raw_data:
            if str(raw_data['type']).lower() != UserAnalytic.ANALYTIC_TYPE:
                raise ValueError("Unrecognized type for UserAnalytic")
        else:
            raise ValueError("UserAnalytic must contain a type")

        if 'project' not in raw_data:
            raise ValueError("UserAnalytic must contain a project")

        if 'timespan' in raw_data:
            if str(raw_data['timespan']['type']).upper() == DefaultTime.DEFAULT_TIME_TYPE:
                timespan = DefaultTime.from_repr(raw_data['timespan'])
            elif str(raw_data['timespan']['type']).upper() == CustomTime.CUSTOM_TIME_TYPE:
                timespan = CustomTime.from_repr(raw_data['timespan'])
            else:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("UserAnalytic must contain a timespan")

        if 'dimension' in raw_data:
            if str(raw_data['dimension']).lower() != UserAnalytic.USER_DIMENSION:
                raise ValueError("Unrecognized dimension for UserAnalytic")
        else:
            raise ValueError("A dimension must be defined in the UserAnalytic object")

        if 'metric' in raw_data:
            if str(raw_data['metric']).lower() not in UserAnalytic.ALLOWED_USER_METRIC_VALUES:
                raise ValueError('Unknown value for metric in the UserAnalytic')
        else:
            raise ValueError('A metric must be defined in the UserAnalytic object')

        return UserAnalytic(timespan, raw_data['project'], raw_data['metric'])


class MessageAnalytic(CommonAnalytic):

    MESSAGE_DIMENSION = "message"
    ALLOWED_MESSAGE_METRIC_VALUES = ["m:from_users", "m:from_bot", "m:responses", "m:notifications", "m:unhandled", "m:segmentation", "r:segmentation"]

    def __init__(self, timespan: Union[DefaultTime, CustomTime], project: str, metric: str):
        super().__init__(timespan, project, self.MESSAGE_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> MessageAnalytic:
        if 'type' in raw_data:
            if str(raw_data['type']).lower() != MessageAnalytic.ANALYTIC_TYPE:
                raise ValueError("Unrecognized type for MessageAnalytic")
        else:
            raise ValueError("MessageAnalytic must contain a type")

        if 'project' not in raw_data:
            raise ValueError("MessageAnalytic must contain a project")

        if 'timespan' in raw_data:
            if str(raw_data['timespan']['type']).upper() == DefaultTime.DEFAULT_TIME_TYPE:
                timespan = DefaultTime.from_repr(raw_data['timespan'])
            elif str(raw_data['timespan']['type']).upper() == CustomTime.CUSTOM_TIME_TYPE:
                timespan = CustomTime.from_repr(raw_data['timespan'])
            else:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("MessageAnalytic must contain a timespan")

        if 'dimension' in raw_data:
            if str(raw_data['dimension']).lower() != MessageAnalytic.MESSAGE_DIMENSION:
                raise ValueError("Unrecognized dimension for MessageAnalytic")
        else:
            raise ValueError("A dimension must be defined in the MessageAnalytic object")

        if 'metric' in raw_data:
            if str(raw_data['metric']).lower() not in MessageAnalytic.ALLOWED_MESSAGE_METRIC_VALUES:
                raise ValueError('Unknown value for metric in the MessageAnalytic')
        else:
            raise ValueError('A metric must be defined in the MessageAnalytic object')

        return MessageAnalytic(timespan, raw_data['project'], raw_data['metric'])


class TaskAnalytic(CommonAnalytic):

    TASK_DIMENSION = "task"
    ALLOWED_TASK_METRIC_VALUES = ["t:total", "t:active", "t:closed", "t:new"]

    def __init__(self, timespan: Union[DefaultTime, CustomTime], project: str, metric: str):
        super().__init__(timespan, project, self.TASK_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> TaskAnalytic:
        if 'type' in raw_data:
            if str(raw_data['type']).lower() != TaskAnalytic.ANALYTIC_TYPE:
                raise ValueError("Unrecognized type for TaskAnalytic")
        else:
            raise ValueError("TaskAnalytic must contain a type")

        if 'project' not in raw_data:
            raise ValueError("TaskAnalytic must contain a project")

        if 'timespan' in raw_data:
            if str(raw_data['timespan']['type']).upper() == DefaultTime.DEFAULT_TIME_TYPE:
                timespan = DefaultTime.from_repr(raw_data['timespan'])
            elif str(raw_data['timespan']['type']).upper() == CustomTime.CUSTOM_TIME_TYPE:
                timespan = CustomTime.from_repr(raw_data['timespan'])
            else:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("TaskAnalytic must contain a timespan")

        if 'dimension' in raw_data:
            if str(raw_data['dimension']).lower() != TaskAnalytic.TASK_DIMENSION:
                raise ValueError("Unrecognized dimension for TaskAnalytic")
        else:
            raise ValueError("A dimension must be defined in the TaskAnalytic object")

        if 'metric' in raw_data:
            if str(raw_data['metric']).lower() not in TaskAnalytic.ALLOWED_TASK_METRIC_VALUES:
                raise ValueError('Unknown value for metric in the TaskAnalytic')
        else:
            raise ValueError('A metric must be defined in the TaskAnalytic object')

        return TaskAnalytic(timespan, raw_data['project'], raw_data['metric'])


class TransactionAnalytic(CommonAnalytic):

    TRANSACTION_DIMENSION = "transaction"
    ALLOWED_TRANSACTION_METRIC_VALUES = ["t:total", "t:segmentation"]

    def __init__(self, timespan: Union[DefaultTime, CustomTime], project: str, metric: str, task_id: str = None):
        super().__init__(timespan, project, self.TRANSACTION_DIMENSION, metric)
        self.task_id = task_id

    def to_repr(self) -> dict:
        representation = super().to_repr()
        representation['taskId'] = self.task_id
        return representation

    @staticmethod
    def from_repr(raw_data: dict) -> TransactionAnalytic:
        if 'type' in raw_data:
            if str(raw_data['type']).lower() != TransactionAnalytic.ANALYTIC_TYPE:
                raise ValueError("Unrecognized type for TransactionAnalytic")
        else:
            raise ValueError("TransactionAnalytic must contain a type")

        if 'project' not in raw_data:
            raise ValueError("TransactionAnalytic must contain a project")

        if 'timespan' in raw_data:
            if str(raw_data['timespan']['type']).upper() == DefaultTime.DEFAULT_TIME_TYPE:
                timespan = DefaultTime.from_repr(raw_data['timespan'])
            elif str(raw_data['timespan']['type']).upper() == CustomTime.CUSTOM_TIME_TYPE:
                timespan = CustomTime.from_repr(raw_data['timespan'])
            else:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("TransactionAnalytic must contain a timespan")

        if 'dimension' in raw_data:
            if str(raw_data['dimension']).lower() != TransactionAnalytic.TRANSACTION_DIMENSION:
                raise ValueError("Unrecognized dimension for TransactionAnalytic")
        else:
            raise ValueError("A dimension must be defined in the TransactionAnalytic object")

        if 'metric' in raw_data:
            if str(raw_data['metric']).lower() not in TransactionAnalytic.ALLOWED_TRANSACTION_METRIC_VALUES:
                raise ValueError('Unknown value for metric in the TransactionAnalytic')
        else:
            raise ValueError('A metric must be defined in the TransactionAnalytic object')

        return TransactionAnalytic(timespan, raw_data['project'], raw_data['metric'], task_id=raw_data.get('taskId'))


class ConversationAnalytic(CommonAnalytic):

    CONVERSATION_DIMENSION = "conversation"
    ALLOWED_CONVERSATION_METRIC_VALUES = ["c:total", "c:new", "c:length", "c:path"]

    def __init__(self, timespan: Union[DefaultTime, CustomTime], project: str, metric: str):
        super().__init__(timespan, project, self.CONVERSATION_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationAnalytic:
        if 'type' in raw_data:
            if str(raw_data['type']).lower() != ConversationAnalytic.ANALYTIC_TYPE:
                raise ValueError("Unrecognized type for ConversationAnalytic")
        else:
            raise ValueError("ConversationAnalytic must contain a type")

        if 'project' not in raw_data:
            raise ValueError("ConversationAnalytic must contain a project")

        if 'timespan' in raw_data:
            if str(raw_data['timespan']['type']).upper() == DefaultTime.DEFAULT_TIME_TYPE:
                timespan = DefaultTime.from_repr(raw_data['timespan'])
            elif str(raw_data['timespan']['type']).upper() == CustomTime.CUSTOM_TIME_TYPE:
                timespan = CustomTime.from_repr(raw_data['timespan'])
            else:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("ConversationAnalytic must contain a timespan")

        if 'dimension' in raw_data:
            if str(raw_data['dimension']).lower() != ConversationAnalytic.CONVERSATION_DIMENSION:
                raise ValueError("Unrecognized dimension for ConversationAnalytic")
        else:
            raise ValueError("A dimension must be defined in the ConversationAnalytic object")

        if 'metric' in raw_data:
            if str(raw_data['metric']).lower() not in ConversationAnalytic.ALLOWED_CONVERSATION_METRIC_VALUES:
                raise ValueError('Unknown value for metric in the ConversationAnalytic')
        else:
            raise ValueError('A metric must be defined in the ConversationAnalytic object')

        return ConversationAnalytic(timespan, raw_data['project'], raw_data['metric'])


class DialogueAnalytic(CommonAnalytic):

    DIALOGUE_DIMENSION = "dialogue"
    ALLOWED_DIALOGUE_METRIC_VALUES = ["d:fallback", "d:intents", "d:domains"]

    def __init__(self, timespan: Union[DefaultTime, CustomTime], project: str, metric: str):
        super().__init__(timespan, project, self.DIALOGUE_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> DialogueAnalytic:
        if 'type' in raw_data:
            if str(raw_data['type']).lower() != DialogueAnalytic.ANALYTIC_TYPE:
                raise ValueError("Unrecognized type for DialogueAnalytic")
        else:
            raise ValueError("DialogueAnalytic must contain a type")

        if 'project' not in raw_data:
            raise ValueError("DialogueAnalytic must contain a project")

        if 'timespan' in raw_data:
            if str(raw_data['timespan']['type']).upper() == DefaultTime.DEFAULT_TIME_TYPE:
                timespan = DefaultTime.from_repr(raw_data['timespan'])
            elif str(raw_data['timespan']['type']).upper() == CustomTime.CUSTOM_TIME_TYPE:
                timespan = CustomTime.from_repr(raw_data['timespan'])
            else:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("DialogueAnalytic must contain a timespan")

        if 'dimension' in raw_data:
            if str(raw_data['dimension']).lower() != DialogueAnalytic.DIALOGUE_DIMENSION:
                raise ValueError("Unrecognized dimension for DialogueAnalytic")
        else:
            raise ValueError("A dimension must be defined in the DialogueAnalytic object")

        if 'metric' in raw_data:
            if str(raw_data['metric']).lower() not in DialogueAnalytic.ALLOWED_DIALOGUE_METRIC_VALUES:
                raise ValueError('Unknown value for metric in the DialogueAnalytic')
        else:
            raise ValueError('A metric must be defined in the DialogueAnalytic object')

        return DialogueAnalytic(timespan, raw_data['project'], raw_data['metric'])


class BotAnalytic(CommonAnalytic):

    BOT_DIMENSION = "bot"
    ALLOWED_BOT_METRIC_VALUES = ["b:response"]

    def __init__(self, timespan: Union[DefaultTime, CustomTime], project: str, metric: str):
        super().__init__(timespan, project, self.BOT_DIMENSION, metric)

    @staticmethod
    def from_repr(raw_data: dict) -> BotAnalytic:
        if 'type' in raw_data:
            if str(raw_data['type']).lower() != BotAnalytic.ANALYTIC_TYPE:
                raise ValueError("Unrecognized type for BotAnalytic")
        else:
            raise ValueError("BotAnalytic must contain a type")

        if 'project' not in raw_data:
            raise ValueError("BotAnalytic must contain a project")

        if 'timespan' in raw_data:
            if str(raw_data['timespan']['type']).upper() == DefaultTime.DEFAULT_TIME_TYPE:
                timespan = DefaultTime.from_repr(raw_data['timespan'])
            elif str(raw_data['timespan']['type']).upper() == CustomTime.CUSTOM_TIME_TYPE:
                timespan = CustomTime.from_repr(raw_data['timespan'])
            else:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("BotAnalytic must contain a timespan")

        if 'dimension' in raw_data:
            if str(raw_data['dimension']).lower() != BotAnalytic.BOT_DIMENSION:
                raise ValueError("Unrecognized dimension for BotAnalytic")
        else:
            raise ValueError("A dimension must be defined in the BotAnalytic object")

        if 'metric' in raw_data:
            if str(raw_data['metric']).lower() not in BotAnalytic.ALLOWED_BOT_METRIC_VALUES:
                raise ValueError('Unknown value for metric in the BotAnalytic')
        else:
            raise ValueError('A metric must be defined in the BotAnalytic object')

        return BotAnalytic(timespan, raw_data['project'], raw_data['metric'])
