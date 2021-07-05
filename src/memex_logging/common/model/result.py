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
    def from_repr(raw_data: dict) -> CommonResult:
        if raw_data['type'] == ConversationLengthAnalyticResult.TYPE:
            return ConversationLengthAnalyticResult.from_repr(raw_data)
        elif raw_data['type'] == ConversationPathAnalyticResult.TYPE:
            return ConversationPathAnalyticResult.from_repr(raw_data)
        elif raw_data['type'] == TransactionAnalyticResult.TYPE:
            return TransactionAnalyticResult.from_repr(raw_data)
        elif raw_data['type'] == SegmentationAnalyticResult.TYPE:
            return SegmentationAnalyticResult.from_repr(raw_data)
        else:
            return AnalyticResult.from_repr(raw_data)


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
        return AnalyticResult(raw_data['count'], raw_data['items'], raw_data['type'])

    def __eq__(self, o) -> bool:
        if isinstance(o, AnalyticResult):
            return o.count == self.count and o.items == self.items and o.item_type == self.item_type
        else:
            return False


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
        return ConversationLength(raw_data['conversationId'], raw_data['length'])

    def __eq__(self, o) -> bool:
        if isinstance(o, ConversationLength):
            return o.conversation_id == self.conversation_id and o.length == self.length
        else:
            return False


class ConversationLengthAnalyticResult(CommonResult):

    TYPE = "length"

    def __init__(self, count: int, lengths: List[ConversationLength]) -> None:
        self.count = count
        self.lengths = lengths

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'lengths': [length.to_repr() for length in self.lengths],
            'type': self.TYPE
        }

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationLengthAnalyticResult:
        if str(raw_data['type']).lower() != ConversationLengthAnalyticResult.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for ConversationLengthAnalyticResult")

        lengths = [ConversationLength.from_repr(length) for length in raw_data['lengths']]
        return ConversationLengthAnalyticResult(raw_data['count'], lengths)

    def __eq__(self, o) -> bool:
        if isinstance(o, ConversationLengthAnalyticResult):
            return o.count == self.count and o.lengths == self.lengths
        else:
            return False


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
        return ConversationPath(raw_data['conversationId'], raw_data['messages'])

    def __eq__(self, o) -> bool:
        if isinstance(o, ConversationPath):
            return o.conversation_id == self.conversation_id and o.messages == self.messages
        else:
            return False


class ConversationPathAnalyticResult(CommonResult):

    TYPE = "path"

    def __init__(self, count: int, paths: List[ConversationPath]) -> None:
        self.count = count
        self.paths = paths

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'paths': [path.to_repr() for path in self.paths],
            'type': self.TYPE
        }

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationPathAnalyticResult:
        if str(raw_data['type']).lower() != ConversationPathAnalyticResult.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for ConversationPathAnalyticResult")

        paths = [ConversationPath.from_repr(path) for path in raw_data['paths']]
        return ConversationPathAnalyticResult(raw_data['count'], paths)

    def __eq__(self, o) -> bool:
        if isinstance(o, ConversationPathAnalyticResult):
            return o.count == self.count and o.paths == self.paths
        else:
            return False


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


class SegmentationAnalyticResult(CommonResult):

    TYPE = "segmentation"

    def __init__(self, counts: List[Segmentation]) -> None:
        self.counts = counts

    def to_repr(self) -> dict:
        return {
            'counts': [count.to_repr() for count in self.counts],
            'type': self.TYPE
        }

    @staticmethod
    def from_repr(raw_data: dict) -> SegmentationAnalyticResult:
        if str(raw_data['type']).lower() != SegmentationAnalyticResult.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for SegmentationAnalyticResult")

        counts = [Segmentation.from_repr(count) for count in raw_data['counts']]
        return SegmentationAnalyticResult(counts)

    def __eq__(self, o) -> bool:
        if isinstance(o, SegmentationAnalyticResult):
            return o.counts == self.counts
        else:
            return False


class TransactionReturn:

    def __init__(self, task_id: str, transaction_ids: List[str]) -> None:
        self.task_id = task_id
        self.transaction_ids = transaction_ids

    def to_repr(self) -> dict:
        return {
            'taskId': self.task_id,
            'transactionIds': self.transaction_ids
        }

    @staticmethod
    def from_repr(raw_data: dict) -> TransactionReturn:
        return TransactionReturn(raw_data['taskId'], raw_data['transactionIds'])

    def __eq__(self, o) -> bool:
        if isinstance(o, TransactionReturn):
            return o.task_id == self.task_id and o.transaction_ids == self.transaction_ids
        else:
            return False


class TransactionAnalyticResult(CommonResult):

    TYPE = "transaction"

    def __init__(self, count: int, transactions: List[TransactionReturn]) -> None:
        self.count = count
        self.transactions = transactions

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'transactions': [transaction.to_repr() for transaction in self.transactions],
            'type': self.TYPE
        }

    @staticmethod
    def from_repr(raw_data: dict) -> TransactionAnalyticResult:
        if str(raw_data['type']).lower() != TransactionAnalyticResult.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for TransactionAnalyticResult")

        transactions = [TransactionReturn.from_repr(transaction) for transaction in raw_data['transactions']]
        return TransactionAnalyticResult(raw_data['count'], transactions)

    def __eq__(self, o) -> bool:
        if isinstance(o, TransactionAnalyticResult):
            return o.count == self.count and o.transactions == self.transactions
        else:
            return False
