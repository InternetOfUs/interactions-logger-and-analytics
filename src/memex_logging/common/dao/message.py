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
from typing import Tuple, List

from elasticsearch import Elasticsearch

from memex_logging.common.dao.conmon import CommonDao, EntryNotFound
from memex_logging.common.model.message import Message


logger = logging.getLogger("logger.common.dao.message")


class MessageDao(CommonDao):

    BASE_INDEX = "message"

    def __init__(self, es: Elasticsearch) -> None:
        super().__init__(es, self.BASE_INDEX)

    def add(self, index: str, message: Message, doc_type: str = "_doc") -> str:
        """
        Add a message to EL

        :param str index: the index where to add the message
        :param Message message: the message to add
        :param str doc_type: the type of the document
        :return: the trace_id of the added massage
        """

        query = self._es.index(index=index, body=message.to_repr(), doc_type=doc_type)
        return query["_id"]

    @staticmethod
    def build_query_by_message_id(message_id: str) -> dict:
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
    def add_user_id_to_query(query: dict, user_id: str) -> dict:
        query["query"]["bool"]["must"].append(
            {
                "match_phrase": {
                    "userId": user_id
                }
            }
        )
        return query

    @staticmethod
    def add_channel_to_query(query: dict, channel: str) -> dict:
        query["query"]["bool"]["must"].append(
            {
                "match_phrase": {
                    "channel": channel
                }
            }
        )
        return query

    @staticmethod
    def add_message_type_to_query(query: dict, message_type: str) -> dict:
        query["query"]["bool"]["must"].append(
            {
                "match_phrase": {
                    "type": message_type
                }
            }
        )
        return query

    def get(self, index: str, query: dict) -> Tuple[Message, str]:
        """
        Retrieve a message from EL

        :param str index: the index
        :param dict query: the query for retrieving the message
        :return: a tuple containing the message and the trace_id of that massage
        :raise EntryNotFound: when could not find any message
        """

        response = self._es.search(index=index, body=query)
        if len(response['hits']['hits']) == 0:
            logger.debug("Could not find any message")
            raise EntryNotFound("Could not find any message")
        else:
            message = Message.from_repr(response['hits']['hits'][0]['_source'])
            return message, response['hits']['hits'][0]['_id']

    def delete(self, index: str, query: dict) -> None:
        """
        Delete a message from EL

        :param str index: the index
        :param dict query: the query for deleting the message
        """

        self._es.delete_by_query(index=index, body=query)

    def search(self, index: str, query: dict) -> List[Message]:
        """
        Search messages in EL

        :param str index: the index
        :param dict query: the query for seaching messages
        :return: a list containing the messages
        """

        response = self._es.search(index=index, body=query)
        return [Message.from_repr(element['_source']) for element in response['hits']['hits']]
