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
from typing import Optional, List, Tuple

from elasticsearch import Elasticsearch

from memex_logging.common.dao.collector import DaoCollector
from memex_logging.common.dao.message import MessageDao
from memex_logging.common.model.message import Message


class MockMessageDao(MessageDao):
    """
    A mock for the message dao
    """

    def add_messages(self, message: Message, doc_type: str = "_doc") -> str:
        pass

    def get_message(self, project: Optional[str] = None, message_id: Optional[str] = None,
                    user_id: Optional[str] = None, trace_id: Optional[str] = None) -> Tuple[Message, str]:
        pass

    def delete_message(self, project: Optional[str] = None, message_id: Optional[str] = None,
                       user_id: Optional[str] = None, trace_id: Optional[str] = None) -> None:
        pass

    def search_messages(self, project: str, from_time: datetime, to_time: datetime, max_size: int,
                        user_id: Optional[str] = None, channel: Optional[str] = None,
                        message_type: Optional[str] = None) -> List[Message]:
        pass


class MockDaoCollectorBuilder:
    """
    The class for building the dao mocks
    """

    @staticmethod
    def build_mock_daos() -> DaoCollector:
        return DaoCollector(
            MockMessageDao(Elasticsearch())
        )
