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
from unittest import TestCase

from elasticsearch import Elasticsearch

from memex_logging.common.dao.message import MessageDao


class TestMessageDao(TestCase):

    def test_build_query_based_on_parameters(self):
        message_dao = MessageDao(Elasticsearch())

        query = message_dao._build_query_based_on_parameters(trace_id="trace_id",  message_id=None, user_id=None)
        self.assertEqual({
            "size": 1,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "_id": "trace_id"
                            }
                        }
                    ]
                }
            }
        }, query)

        query = message_dao._build_query_based_on_parameters(trace_id=None,  message_id="message_id", user_id="user_id")
        self.assertEqual({
            "size": 1,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "messageId.keyword": "message_id"
                            }
                        },
                        {
                            "match_phrase": {
                                "userId.keyword": "user_id"
                            }
                        }
                    ]
                }
            }
        }, query)

        with self.assertRaises(ValueError):
            message_dao._build_query_based_on_parameters(trace_id=None, message_id=None, user_id=None)

        with self.assertRaises(ValueError):
            message_dao._build_query_based_on_parameters(trace_id=None, message_id="message_id", user_id=None)

        with self.assertRaises(ValueError):
            message_dao._build_query_based_on_parameters(trace_id=None, message_id=None, user_id="user_id")

    def test_generate_index_based_on_time_range(self):
        message_dao = MessageDao(Elasticsearch())

        with self.assertRaises(ValueError):
            message_dao._generate_index_based_on_time_range("project", datetime(2021, 2, 6), datetime(2021, 2, 5))

        index = message_dao._generate_index_based_on_time_range("project", datetime(2021, 2, 4), datetime(2021, 2, 5))
        self.assertEqual(f"{MessageDao.BASE_INDEX}-project-*", index)
