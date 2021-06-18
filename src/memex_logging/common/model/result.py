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
from typing import List


class CommonResult(ABC):

    @abstractmethod
    def to_repr(self) -> dict:
        pass

    @staticmethod
    def from_repr(raw_data) -> CommonResult:
        if 'count' in raw_data and 'items' in raw_data and 'type' in raw_data:
            return AnalyticResult.from_repr(raw_data)
        elif 'count' in raw_data and 'lengths' in raw_data and 'type' in raw_data:
            return ConversationLengthAnalyticResult.from_repr(raw_data)
        elif 'count' in raw_data and 'paths' in raw_data and 'type' in raw_data:
            return ConversationPathAnalyticResult.from_repr(raw_data)
        elif 'counts' in raw_data and 'type' in raw_data:
            return ConversationLengthAnalyticResult.from_repr(raw_data)
        else:
            raise ValueError("Unrecognized dimension for Analytic")


class AnalyticResult(CommonResult):

    def __init__(self, count: int, items: List[str], item_type: str) -> None:
        self.count = count
        self.items = items
        self.item_type = item_type

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'items': self.items,
            'type': self.item_type
        }

    @staticmethod
    def from_repr(raw_data: dict) -> AnalyticResult:
        if 'count' not in raw_data:
            raise ValueError("AnalyticResult must contain a count field")

        if 'items' not in raw_data:
            raise ValueError("AnalyticResult must contain a items field")

        if 'type' not in raw_data:
            raise ValueError("AnalyticResult must contain a type field")

        return AnalyticResult(raw_data['count'], raw_data['items'], raw_data['type'])


class ConversationLength:

    def __init__(self, conversation_id: str, length: int) -> None:
        self.conversation_id = conversation_id
        self.length = length

    def to_repr(self) -> dict:
        return {
            'conversationId': self.conversation_id,
            'length': self.length
        }

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationLength:
        if 'conversationId' not in raw_data:
            raise ValueError("ConversationLength must contain a conversationId field")

        if 'length' not in raw_data:
            raise ValueError("ConversationLength must contain a length field")

        return ConversationLength(raw_data['conversationId'], raw_data['length'])


class ConversationLengthAnalyticResult(CommonResult):

    def __init__(self, count: int, lengths: List[ConversationLength], item_type: str) -> None:
        self.count = count
        self.lengths = lengths
        self.item_type = item_type

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'lengths': [length.to_repr() for length in self.lengths],
            'type': self.item_type
        }

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationLengthAnalyticResult:
        if 'count' not in raw_data:
            raise ValueError("ConversationLengthAnalyticResult must contain a count field")

        if 'lengths' in raw_data:
            lengths = [ConversationLength.from_repr(length) for length in raw_data['lengths']]
        else:
            raise ValueError("ConversationLengthAnalyticResult must contain a lengths field")

        if 'type' not in raw_data:
            raise ValueError("ConversationLengthAnalyticResult must contain a type field")

        return ConversationLengthAnalyticResult(raw_data['count'], lengths, raw_data['type'])


class ConversationPath:

    def __init__(self, conversation_id: str, messages: List[str]) -> None:
        self.conversation_id = conversation_id
        self.messages = messages

    def to_repr(self) -> dict:
        return {
            'conversationId': self.conversation_id,
            'messages': self.messages
        }

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationPath:
        if 'conversationId' not in raw_data:
            raise ValueError("ConversationPath must contain a conversationId field")

        if 'messages' not in raw_data:
            raise ValueError("ConversationPath must contain a messages field")

        return ConversationPath(raw_data['conversationId'], raw_data['messages'])


class ConversationPathAnalyticResult(CommonResult):

    def __init__(self, count: int, paths: List[ConversationPath], item_type: str) -> None:
        self.count = count
        self.paths = paths
        self.item_type = item_type

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'paths': [path.to_repr() for path in self.paths],
            'type': self.item_type
        }

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationPathAnalyticResult:
        if 'count' not in raw_data:
            raise ValueError("AnalyticResult must contain a count field")

        if 'paths' in raw_data:
            paths = [ConversationPath.from_repr(path) for path in raw_data['paths']]
        else:
            raise ValueError("ConversationLengthAnalyticResult must contain a lengths field")

        if 'type' not in raw_data:
            raise ValueError("AnalyticResult must contain a type field")

        return ConversationPathAnalyticResult(raw_data['count'], paths, raw_data['type'])


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
        if 'count' not in raw_data:
            raise ValueError("Segmentation must contain a count field")

        if 'type' not in raw_data:
            raise ValueError("Segmentation must contain a type field")

        return Segmentation(raw_data['count'], raw_data['type'])


class SegmentationAnalyticResult(CommonResult):

    def __init__(self, counts: List[Segmentation], count_type: str) -> None:
        self.counts = counts
        self.count_type = count_type

    def to_repr(self) -> dict:
        return {
            'counts': [count.to_repr() for count in self.counts],
            'type': self.count_type
        }

    @staticmethod
    def from_repr(raw_data: dict) -> SegmentationAnalyticResult:
        if 'counts' in raw_data:
            counts = [Segmentation.from_repr(count) for count in raw_data['counts']]
        else:
            raise ValueError("SegmentationAnalyticResult must contain a counts field")

        if 'type' not in raw_data:
            raise ValueError("SegmentationAnalyticResult must contain a type field")

        return SegmentationAnalyticResult(counts, raw_data['type'])
