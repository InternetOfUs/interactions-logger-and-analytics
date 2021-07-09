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
from datetime import datetime
from typing import List, Union, Optional


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

    def __init__(self, count: int, items: List[str], item_type: str, creation_datetime: Optional[datetime], from_datetime: Optional[datetime], to_datetime: Optional[datetime]) -> None:
        self.count = count
        self.items = items
        self.item_type = item_type
        self.creation_datetime = creation_datetime
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'items': self.items,
            'type': self.item_type,
            'creationDt': self.creation_datetime.isoformat() if self.creation_datetime is not None else None,
            'fromDt': self.from_datetime.isoformat() if self.from_datetime is not None else None,
            'toDt': self.to_datetime.isoformat() if self.to_datetime is not None else None
        }

    @staticmethod
    def from_repr(raw_data: dict) -> AnalyticResult:
        return AnalyticResult(
            raw_data['count'],
            raw_data['items'],
            raw_data['type'],
            datetime.fromisoformat(raw_data.get('creationDt')) if raw_data.get('creationDt') is not None else None,
            datetime.fromisoformat(raw_data.get('fromDt')) if raw_data.get('fromDt') is not None else None,
            datetime.fromisoformat(raw_data.get('toDt')) if raw_data.get('toDt') is not None else None
        )

    def __eq__(self, o) -> bool:
        if isinstance(o, AnalyticResult):
            return o.count == self.count and o.items == self.items and o.item_type == self.item_type and o.creation_datetime == self.creation_datetime \
                   and o.from_datetime == self.from_datetime and o.to_datetime == self.to_datetime
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

    def __init__(self, count: int, lengths: List[ConversationLength], creation_datetime: Optional[datetime], from_datetime: Optional[datetime], to_datetime: Optional[datetime]) -> None:
        self.count = count
        self.lengths = lengths
        self.creation_datetime = creation_datetime
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'lengths': [length.to_repr() for length in self.lengths],
            'type': self.TYPE,
            'creationDt': self.creation_datetime.isoformat() if self.creation_datetime is not None else None,
            'fromDt': self.from_datetime.isoformat() if self.from_datetime is not None else None,
            'toDt': self.to_datetime.isoformat() if self.to_datetime is not None else None
        }

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationLengthAnalyticResult:
        if str(raw_data['type']).lower() != ConversationLengthAnalyticResult.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for ConversationLengthAnalyticResult")

        lengths = [ConversationLength.from_repr(length) for length in raw_data['lengths']]
        return ConversationLengthAnalyticResult(
            raw_data['count'],
            lengths,
            datetime.fromisoformat(raw_data.get('creationDt')) if raw_data.get('creationDt') is not None else None,
            datetime.fromisoformat(raw_data.get('fromDt')) if raw_data.get('fromDt') is not None else None,
            datetime.fromisoformat(raw_data.get('toDt')) if raw_data.get('toDt') is not None else None
        )

    def __eq__(self, o) -> bool:
        if isinstance(o, ConversationLengthAnalyticResult):
            return o.count == self.count and o.lengths == self.lengths and o.creation_datetime == self.creation_datetime \
                   and o.from_datetime == self.from_datetime and o.to_datetime == self.to_datetime
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

    def __init__(self, count: int, paths: List[ConversationPath], creation_datetime: Optional[datetime], from_datetime: Optional[datetime], to_datetime: Optional[datetime]) -> None:
        self.count = count
        self.paths = paths
        self.creation_datetime = creation_datetime
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'paths': [path.to_repr() for path in self.paths],
            'type': self.TYPE,
            'creationDt': self.creation_datetime.isoformat() if self.creation_datetime is not None else None,
            'fromDt': self.from_datetime.isoformat() if self.from_datetime is not None else None,
            'toDt': self.to_datetime.isoformat() if self.to_datetime is not None else None
        }

    @staticmethod
    def from_repr(raw_data: dict) -> ConversationPathAnalyticResult:
        if str(raw_data['type']).lower() != ConversationPathAnalyticResult.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for ConversationPathAnalyticResult")

        paths = [ConversationPath.from_repr(path) for path in raw_data['paths']]
        return ConversationPathAnalyticResult(
            raw_data['count'],
            paths,
            datetime.fromisoformat(raw_data.get('creationDt')) if raw_data.get('creationDt') is not None else None,
            datetime.fromisoformat(raw_data.get('fromDt')) if raw_data.get('fromDt') is not None else None,
            datetime.fromisoformat(raw_data.get('toDt')) if raw_data.get('toDt') is not None else None
        )

    def __eq__(self, o) -> bool:
        if isinstance(o, ConversationPathAnalyticResult):
            return o.count == self.count and o.paths == self.paths and o.creation_datetime == self.creation_datetime \
                   and o.from_datetime == self.from_datetime and o.to_datetime == self.to_datetime
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

    def __init__(self, counts: List[Segmentation], creation_datetime: Optional[datetime], from_datetime: Optional[datetime], to_datetime: Optional[datetime]) -> None:
        self.counts = counts
        self.creation_datetime = creation_datetime
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime

    def to_repr(self) -> dict:
        return {
            'counts': [count.to_repr() for count in self.counts],
            'type': self.TYPE,
            'creationDt': self.creation_datetime.isoformat() if self.creation_datetime is not None else None,
            'fromDt': self.from_datetime.isoformat() if self.from_datetime is not None else None,
            'toDt': self.to_datetime.isoformat() if self.to_datetime is not None else None
        }

    @staticmethod
    def from_repr(raw_data: dict) -> SegmentationAnalyticResult:
        if str(raw_data['type']).lower() != SegmentationAnalyticResult.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for SegmentationAnalyticResult")

        counts = [Segmentation.from_repr(count) for count in raw_data['counts']]
        return SegmentationAnalyticResult(
            counts,
            datetime.fromisoformat(raw_data.get('creationDt')) if raw_data.get('creationDt') is not None else None,
            datetime.fromisoformat(raw_data.get('fromDt')) if raw_data.get('fromDt') is not None else None,
            datetime.fromisoformat(raw_data.get('toDt')) if raw_data.get('toDt') is not None else None
        )

    def __eq__(self, o) -> bool:
        if isinstance(o, SegmentationAnalyticResult):
            return o.counts == self.counts and o.creation_datetime == self.creation_datetime \
                   and o.from_datetime == self.from_datetime and o.to_datetime == self.to_datetime
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

    def __init__(self, count: int, transactions: List[TransactionReturn], creation_datetime: Optional[datetime], from_datetime: Optional[datetime], to_datetime: Optional[datetime]) -> None:
        self.count = count
        self.transactions = transactions
        self.creation_datetime = creation_datetime
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime

    def to_repr(self) -> dict:
        return {
            'count': self.count,
            'transactions': [transaction.to_repr() for transaction in self.transactions],
            'type': self.TYPE,
            'creationDt': self.creation_datetime.isoformat() if self.creation_datetime is not None else None,
            'fromDt': self.from_datetime.isoformat() if self.from_datetime is not None else None,
            'toDt': self.to_datetime.isoformat() if self.to_datetime is not None else None
        }

    @staticmethod
    def from_repr(raw_data: dict) -> TransactionAnalyticResult:
        if str(raw_data['type']).lower() != TransactionAnalyticResult.TYPE:
            raise ValueError(f"Unrecognized type [{raw_data['type']}] for TransactionAnalyticResult")

        transactions = [TransactionReturn.from_repr(transaction) for transaction in raw_data['transactions']]
        return TransactionAnalyticResult(
            raw_data['count'],
            transactions,
            datetime.fromisoformat(raw_data.get('creationDt')) if raw_data.get('creationDt') is not None else None,
            datetime.fromisoformat(raw_data.get('fromDt')) if raw_data.get('fromDt') is not None else None,
            datetime.fromisoformat(raw_data.get('toDt')) if raw_data.get('toDt') is not None else None
        )

    def __eq__(self, o) -> bool:
        if isinstance(o, TransactionAnalyticResult):
            return o.count == self.count and o.transactions == self.transactions and o.creation_datetime == self.creation_datetime \
                   and o.from_datetime == self.from_datetime and o.to_datetime == self.to_datetime
        else:
            return False


class AggregationResult:

    def __init__(self, aggregation: str, aggregation_result: Union[int, float, dict], creation_datetime: Optional[datetime], from_datetime: Optional[datetime], to_datetime: Optional[datetime]) -> None:
        self.aggregation = aggregation
        self.aggregation_result = aggregation_result
        self.creation_datetime = creation_datetime
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime

    def to_repr(self) -> dict:
        return {
            self.aggregation: self.aggregation_result,
            'creationDt': self.creation_datetime.isoformat() if self.creation_datetime is not None else None,
            'fromDt': self.from_datetime.isoformat() if self.from_datetime is not None else None,
            'toDt': self.to_datetime.isoformat() if self.to_datetime is not None else None
        }

    @staticmethod
    def from_repr(raw_data: dict, aggregation: str) -> AggregationResult:
        return AggregationResult(
            aggregation,
            raw_data[aggregation.lower()],
            datetime.fromisoformat(raw_data.get('creationDt')) if raw_data.get('creationDt') is not None else None,
            datetime.fromisoformat(raw_data.get('fromDt')) if raw_data.get('fromDt') is not None else None,
            datetime.fromisoformat(raw_data.get('toDt')) if raw_data.get('toDt') is not None else None
        )

    def __eq__(self, o) -> bool:
        if isinstance(o, AggregationResult):
            return o.aggregation == self.aggregation and o.aggregation_result == self.aggregation_result and o.creation_datetime == self.creation_datetime \
                   and o.from_datetime == self.from_datetime and o.to_datetime == self.to_datetime
        else:
            return False

