from __future__ import absolute_import, annotations

from datetime import datetime
from unittest import TestCase

from memex_logging.common.dao.message import MessageDao


class TestMessageDao(TestCase):

    def test_build_query_based_on_parameters(self):
        message_dao = MessageDao(None)

        query = message_dao._build_query_based_on_parameters(trace_id="trace_id",  message_id=None, user_id=None)
        self.assertEqual({
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
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "messageId": "message_id"
                            }
                        },
                        {
                            "match_phrase": {
                                "userId": "user_id"
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
        message_dao = MessageDao(None)

        with self.assertRaises(ValueError):
            message_dao._generate_index_based_on_time_range("project", datetime(2021, 2, 6), datetime(2021, 2, 5))

        index = message_dao._generate_index_based_on_time_range("project", datetime(2021, 2, 5), datetime(2021, 2, 5))
        self.assertEqual(f"{MessageDao.BASE_INDEX}-project-2021-02-05", index)

        index = message_dao._generate_index_based_on_time_range("project", datetime(2021, 2, 4), datetime(2021, 2, 5))
        self.assertEqual(f"{MessageDao.BASE_INDEX}-project-*", index)

