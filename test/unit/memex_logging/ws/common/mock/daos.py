from __future__ import absolute_import, annotations

from datetime import datetime
from typing import Optional, List, Tuple

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

    def search_messages(self, project: str, from_time: datetime, to_time: datetime, user_id: Optional[str] = None,
                        channel: Optional[str] = None, message_type: Optional[str] = None) -> List[Message]:
        pass


class MockDaoCollectorBuilder:
    """
    The class for building the dao mocks
    """

    @staticmethod
    def build_mock_daos() -> DaoCollector:
        return DaoCollector(
            MockMessageDao(None)
        )
