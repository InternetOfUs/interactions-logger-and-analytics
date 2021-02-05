# Copyright 2020 U-Hopper srl
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

import logging
from datetime import datetime
from typing import Tuple, List, Optional

from elasticsearch import Elasticsearch

from memex_logging.common.dao.conmon import CommonDao
from memex_logging.common.model.message import Message


logger = logging.getLogger("logger.common.dao.message")


class MessageDao(CommonDao):
    """
    A dao for the management of messages
    """

    BASE_INDEX = "message"

    def __init__(self, es: Elasticsearch) -> None:
        """
        :param Elasticsearch es: a connector for Elasticsearch
        """
        super().__init__(es, self.BASE_INDEX)

    @staticmethod
    def _build_query_by_message_id(message_id: str) -> dict:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "messageId": message_id
                            }
                        }
                    ]
                }
            }
        }

    @staticmethod
    def _add_user_id_to_query(query: dict, user_id: str) -> dict:
        query["query"]["bool"]["must"].append(
            {
                "match_phrase": {
                    "userId": user_id
                }
            }
        )
        return query

    @staticmethod
    def _add_channel_to_query(query: dict, channel: str) -> dict:
        query["query"]["bool"]["must"].append(
            {
                "match_phrase": {
                    "channel": channel
                }
            }
        )
        return query

    @staticmethod
    def _add_message_type_to_query(query: dict, message_type: str) -> dict:
        query["query"]["bool"]["must"].append(
            {
                "match_phrase": {
                    "type": message_type
                }
            }
        )
        return query

    def add_message(self, message: Message, doc_type: str = "_doc") -> str:
        """
        Add a message to Elasticsearch

        :param Message message: the message to add
        :param str doc_type: the type of the document
        :return: the trace_id of the added massage
        """

        index = self._generate_index(message.project, message.timestamp)
        return self.add(index, message.to_repr(), doc_type=doc_type)

    def get_message(self, project: Optional[str] = None, message_id: Optional[str] = None,
                    user_id: Optional[str] = None, trace_id: Optional[str] = None) -> Tuple[Message, str]:
        """
        Retrieve a message from Elasticsearch specifying only the `trace_id` or the the `message_id` and the `user_id`

        :param Optional[str] project: the project from which to retrieve the message
        :param Optional[str] message_id: the id of the message to retrieve
        :param Optional[str] user_id: the id of the user of the message to retrieve
        :param Optional[str] trace_id: the trace_id of the message to retrieve
        :return: a tuple containing the message and the trace_id of that massage
        :raise EntryNotFound: when could not find any message
        """

        index = self._generate_index(project=project)

        if message_id and user_id:
            query = self._build_query_by_message_id(message_id)
            query = self._add_user_id_to_query(query, user_id)
        elif trace_id:
            query = self._build_query_by_id(trace_id)
        else:
            raise ValueError("Missing required parameter, you have to specify only the `trace_id` or the the `message_id` and the `user_id`")

        response = self.get(index, query)
        message = Message.from_repr(response[0])
        trace_id = response[1]
        return message, trace_id

    def delete_message(self, project: Optional[str] = None, message_id: Optional[str] = None,
                       user_id: Optional[str] = None, trace_id: Optional[str] = None) -> None:
        """
        Delete a message from Elasticsearch specifying only the `trace_id` or the the `message_id` and the `user_id`

        :param Optional[str] project: the project from which to delete the message
        :param Optional[str] message_id: the id of the message to delete
        :param Optional[str] user_id: the id of the user of the message to delete
        :param Optional[str] trace_id: the trace_id of the message to delete
        """

        index = self._generate_index(project=project)

        if message_id and user_id:
            query = self._build_query_by_message_id(message_id)
            query = self._add_user_id_to_query(query, user_id)
        elif trace_id:
            query = self._build_query_by_id(trace_id)
        else:
            raise ValueError("Missing required parameter, you have to specify only the `trace_id` or the the `message_id` and the `user_id`")

        self.delete(index, query)

    def search_message(self, project: str, from_time: datetime, to_time: datetime, user_id: Optional[str] = None,
                       channel: Optional[str] = None, message_type: Optional[str] = None) -> List[Message]:
        """
        Search messages in Elasticsearch

        :param str project: the project from which to search for messages
        :param datetime from_time: the time from which to search for messages
        :param datetime to_time: the time up to which to search for messages
        :param Optional[str] user_id: the id of the user to search for messages for
        :param Optional[str] channel: the channel from which to search for messages
        :param Optional[str] message_type: the type of the messages to search for
        :return: a list containing the messages
        """

        if from_time > to_time:
            raise ValueError("`fromTime` is greater than `toTime`")
        elif from_time.date() == to_time.date():
            index = self._generate_index(project=project, dt=from_time)
        else:
            index = self._generate_index(project=project)

        query = self._build_time_range_query(from_time, to_time)

        if user_id:
            query = self._add_user_id_to_query(query, user_id)

        if channel:
            query = self._add_channel_to_query(query, channel)

        if message_type:
            query = self._add_message_type_to_query(query, message_type)

        messages_repr = self.search(index, query)
        return [Message.from_repr(message_repr) for message_repr in messages_repr]
