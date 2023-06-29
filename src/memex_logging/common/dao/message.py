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

import logging
from datetime import datetime
from typing import Tuple, List, Optional

from elasticsearch import Elasticsearch

from memex_logging.common.dao.common import CommonDao
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
            "size": 1,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "messageId.keyword": message_id
                            }
                        }
                    ]
                }
            }
        }

    @staticmethod
    def _build_query_by_user_id(user_id: str) -> dict:
        return {
            "size": 1, # TODO check
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "userId.keyword": user_id
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
                    "userId.keyword": user_id
                }
            }
        )
        return query

    @staticmethod
    def _add_channel_to_query(query: dict, channel: str) -> dict:
        query["query"]["bool"]["must"].append(
            {
                "match_phrase": {
                    "channel.keyword": channel
                }
            }
        )
        return query

    @staticmethod
    def _add_message_type_to_query(query: dict, message_type: str) -> dict:
        query["query"]["bool"]["must"].append(
            {
                "match_phrase": {
                    "type.keyword": message_type
                }
            }
        )
        return query

    def add(self, message: Message) -> str:
        """
        Add a message to Elasticsearch

        :param Message message: the message to add
        :return: the trace_id of the added massage
        """

        index = self._generate_index(dt=message.timestamp)
        return self._add_document(index, message.to_repr())

    def _build_query_based_on_parameters(self, trace_id: Optional[str] = None, message_id: Optional[str] = None, user_id: Optional[str] = None) -> dict:
        """
        Build query for Elasticsearch based on parameters, specifying only the `trace_id` or the `message_id` and the `user_id`, or the `user_id` only

        :param Optional[str] trace_id: the trace_id of the message to retrieve
        :param Optional[str] message_id: the id of the message to retrieve
        :param Optional[str] user_id: the id of the user of the message to retrieve
        :return: the query based on the parameters
        :raise EntryNotFound: when could not find any message
        :raise ValueError: when specified neither the `trace_id` or the `message_id` and the `user_id`
        """

        if trace_id:
            return self._build_query_by_id(trace_id)
        elif message_id and user_id:
            query = self._build_query_by_message_id(message_id)
            return self._add_user_id_to_query(query, user_id)
        # elif user_id is not None:
        #     return self._build_query_by_user_id(user_id)
        else:
            raise ValueError("Missing required parameter: you have to specify only the `trace_id` or the `message_id` and the `user_id`")

    def get(self, project: Optional[str] = None, message_id: Optional[str] = None,
            user_id: Optional[str] = None, trace_id: Optional[str] = None) -> Tuple[Message, str]:  # TODO check the usage of trace_id in messages
        """
        Retrieve a message from Elasticsearch specifying only the `trace_id` or the `project`, the `message_id` and the `user_id`

        :param Optional[str] project: the project from which to retrieve the message
        :param Optional[str] message_id: the id of the message to retrieve
        :param Optional[str] user_id: the id of the user of the message to retrieve
        :param Optional[str] trace_id: the trace_id of the message to retrieve
        :return: a tuple containing the message and the trace_id of that massage
        :raise EntryNotFound: when could not find any message
        :raise ValueError: when specified neither the `trace_id` or the `message_id` and the `user_id`
        """

        index = self._generate_index()
        query = self._build_query_based_on_parameters(trace_id=trace_id, message_id=message_id, user_id=user_id)
        if project:
            query = self._add_project_to_query(query, project)
        raw_message, trace_id, index, doc_type = self._get_document(index, query)
        message = Message.from_repr(raw_message)
        return message, trace_id

    def delete(self, project: Optional[str] = None, message_id: Optional[str] = None,
               user_id: Optional[str] = None, trace_id: Optional[str] = None) -> None:
        """
        Delete a message from Elasticsearch specifying only the `trace_id` or the `project`, the `message_id` and the `user_id`

        :param Optional[str] project: the project from which to delete the message
        :param Optional[str] message_id: the id of the message to delete
        :param Optional[str] user_id: the id of the user of the message to delete
        :param Optional[str] trace_id: the trace_id of the message to delete
        :raise ValueError: when specified neither the `trace_id` or the `message_id` and the `user_id`
        """

        index = self._generate_index()
        query = self._build_query_based_on_parameters(trace_id=trace_id, message_id=message_id, user_id=user_id)
        if project:
            query = self._add_project_to_query(query, project)
        self._delete_document(index, query)

    def delete_by_user(self, user_id: str):
        index = self._generate_index()
        query = self._build_query_by_user_id(user_id)
        self._delete_document(index, query)

    def search(self, project: str, from_time: datetime, to_time: datetime, max_size: int, user_id: Optional[str] = None,
               channel: Optional[str] = None, message_type: Optional[str] = None) -> List[Message]:
        """
        Search messages in Elasticsearch

        :param str project: the project from which to search for messages
        :param datetime from_time: the time from which to search for messages
        :param datetime to_time: the time up to which to search for messages
        :param int max_size: the maximum number of messages to retrieve
        :param Optional[str] user_id: the id of the user to search for messages for
        :param Optional[str] channel: the channel from which to search for messages
        :param Optional[str] message_type: the type of the messages to search for
        :return: a list containing the messages
        :raise ValueError: when `fromTime` is greater than `toTime`
        """

        if from_time > to_time:
            raise ValueError("`fromTime` is greater than `toTime`: `fromTime` must be is smaller than `toTime`")

        index = self._generate_index()
        query = self._build_time_range_query(from_time, to_time, max_size)
        query = self._add_project_to_query(query, project)

        if user_id:
            query = self._add_user_id_to_query(query, user_id)

        if channel:
            query = self._add_channel_to_query(query, channel)

        if message_type:
            query = self._add_message_type_to_query(query, message_type)

        messages_repr = self._search_documents(index, query)
        if len(messages_repr) == max_size:
            logger.warning(f"The number of messages retrieved has reached the maximum size of `{max_size}`")
        return [Message.from_repr(message_repr) for message_repr in messages_repr]
