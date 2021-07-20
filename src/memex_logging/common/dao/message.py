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

    def add_messages(self, message: Message, doc_type: str = "_doc") -> str:
        """
        Add a message to Elasticsearch

        :param Message message: the message to add
        :param str doc_type: the type of the document
        :return: the trace_id of the added massage
        """

        index = self._generate_index(project=message.project, dt=message.timestamp)
        return self.add(index, message.to_repr(), doc_type=doc_type)

    def _build_query_based_on_parameters(self, trace_id: Optional[str] = None, message_id: Optional[str] = None, user_id: Optional[str] = None) -> dict:
        """
        Build query for Elasticsearch based on parameters, specifying only the `trace_id` or the `message_id` and the `user_id`

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
        else:
            raise ValueError("Missing required parameter: you have to specify only the `trace_id` or the `message_id` and the `user_id`")

    def get_message(self, project: Optional[str] = None, message_id: Optional[str] = None,
                    user_id: Optional[str] = None, trace_id: Optional[str] = None) -> Tuple[Message, str]:
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

        index = self._generate_index(project=project)
        query = self._build_query_based_on_parameters(trace_id=trace_id, message_id=message_id, user_id=user_id)
        if project:
            query = self._add_project_to_query(query, project)
        response = self.get(index, query)
        message = Message.from_repr(response[0])
        trace_id = response[1]
        return message, trace_id

    def delete_message(self, project: Optional[str] = None, message_id: Optional[str] = None,
                       user_id: Optional[str] = None, trace_id: Optional[str] = None) -> None:
        """
        Delete a message from Elasticsearch specifying only the `trace_id` or the `project`, the `message_id` and the `user_id`

        :param Optional[str] project: the project from which to delete the message
        :param Optional[str] message_id: the id of the message to delete
        :param Optional[str] user_id: the id of the user of the message to delete
        :param Optional[str] trace_id: the trace_id of the message to delete
        :raise ValueError: when specified neither the `trace_id` or the `message_id` and the `user_id`
        """

        index = self._generate_index(project=project)
        query = self._build_query_based_on_parameters(trace_id=trace_id, message_id=message_id, user_id=user_id)
        if project:
            query = self._add_project_to_query(query, project)
        self.delete(index, query)

    def _generate_index_based_on_time_range(self, project: str, from_time: datetime, to_time: datetime) -> str:
        """
        Generate the Elasticsearch index associated to the message based on time range

        :param str project: the project associated to the message
        :param datetime from_time: the time from which to search for messages
        :param datetime to_time: the time up to which to search for messages
        :return: the generated Elasticsearch index
        :raise ValueError: when `fromTime` is greater than `toTime`
        """

        if from_time > to_time:
            raise ValueError("`fromTime` is greater than `toTime`: `fromTime` must be is smaller than `toTime`")
        else:
            return self._generate_index(project=project)

    def search_messages(self, project: str, from_time: datetime, to_time: datetime, max_size: int,
                        user_id: Optional[str] = None, channel: Optional[str] = None,
                        message_type: Optional[str] = None) -> List[Message]:
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

        index = self._generate_index_based_on_time_range(project, from_time, to_time)
        query = self._build_time_range_query(from_time, to_time, max_size)
        query = self._add_project_to_query(query, project)

        if user_id:
            query = self._add_user_id_to_query(query, user_id)

        if channel:
            query = self._add_channel_to_query(query, channel)

        if message_type:
            query = self._add_message_type_to_query(query, message_type)

        messages_repr = self.search(index, query)
        if len(messages_repr) == max_size:
            logger.warning(f"The number of messages retrieved has reached the maximum size of `{max_size}`")
        return [Message.from_repr(message_repr) for message_repr in messages_repr]
