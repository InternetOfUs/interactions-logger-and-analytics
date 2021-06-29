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
from unittest.mock import Mock

from elasticsearch import Elasticsearch
from wenet.interface.client import ApikeyClient
from wenet.interface.task_manager import TaskManagerInterface
from wenet.model.task.task import Task, TaskGoal
from wenet.model.task.transaction import TaskTransaction

from memex_logging.common.analytic.analytic import AnalyticComputation
from memex_logging.common.model.analytic import UserAnalytic, MessageAnalytic, TaskAnalytic, TransactionAnalytic
from memex_logging.common.model.result import TransactionReturn, Segmentation
from memex_logging.common.model.time import CustomTime


class TestAnalyticComputation(TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.es = Elasticsearch()
        self.task_manager_interface = TaskManagerInterface(ApikeyClient("apikey"), "platform_url")
        self.time_range = CustomTime(datetime(2021, 1, 1), datetime(2021, 12, 31))

    def test_compute_u_total(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}, 'type_count': {'value': 0}}})
        total_users = AnalyticComputation.compute_u_total(UserAnalytic(self.time_range, "project", "u:total"), self.es)
        self.assertEqual(0, total_users.count)
        self.assertEqual([], total_users.items)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'user-id-1', 'doc_count': 1}]}, 'type_count': {'value': 1}}})
        total_users = AnalyticComputation.compute_u_total(UserAnalytic(self.time_range, "project", "u:total"), self.es)
        self.assertEqual(1, total_users.count)
        self.assertEqual(['user-id-1'], total_users.items)

    def test_compute_u_active(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}, 'type_count': {'value': 0}}})
        active_users = AnalyticComputation.compute_u_active(UserAnalytic(self.time_range, "project", "u:active"), self.es)
        self.assertEqual(0, active_users.count)
        self.assertEqual([], active_users.items)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'user-id-1', 'doc_count': 1}]}, 'type_count': {'value': 1}}})
        active_users = AnalyticComputation.compute_u_active(UserAnalytic(self.time_range, "project", "u:active"), self.es)
        self.assertEqual(1, active_users.count)
        self.assertEqual(['user-id-1'], active_users.items)

    def test_compute_u_engaged(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}, 'type_count': {'value': 0}}})
        engaged_users = AnalyticComputation.compute_u_engaged(UserAnalytic(self.time_range, "project", "u:engaged"), self.es)
        self.assertEqual(0, engaged_users.count)
        self.assertEqual([], engaged_users.items)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'user-id-1', 'doc_count': 1}]}, 'type_count': {'value': 1}}})
        engaged_users = AnalyticComputation.compute_u_engaged(UserAnalytic(self.time_range, "project", "u:engaged"), self.es)
        self.assertEqual(1, engaged_users.count)
        self.assertEqual(['user-id-1'], engaged_users.items)

    def test_compute_u_new(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}, 'type_count': {'value': 0}}})
        new_users = AnalyticComputation.compute_u_new(UserAnalytic(self.time_range, "project", "u:new"), self.es)
        self.assertEqual(0, new_users.count)
        self.assertEqual([], new_users.items)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'user-id-1', 'doc_count': 1}]}, 'type_count': {'value': 1}}})
        new_users = AnalyticComputation.compute_u_new(UserAnalytic(self.time_range, "project", "u:new"), self.es)
        self.assertEqual(0, new_users.count)
        self.assertEqual([], new_users.items)

        self.es.search = Mock(side_effect=[
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'user-id-1', 'doc_count': 1}]}, 'type_count': {'value': 1}}},
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}, 'type_count': {'value': 0}}},
        ])
        new_users = AnalyticComputation.compute_u_new(UserAnalytic(self.time_range, "project", "u:new"), self.es)
        self.assertEqual(1, new_users.count)
        self.assertEqual(['user-id-1'], new_users.items)

    def test_compute_m_from_users(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        request_messages = AnalyticComputation.compute_m_from_users(MessageAnalytic(self.time_range, "project", "m:from_users"), self.es)
        self.assertEqual(0, request_messages.count)
        self.assertEqual([], request_messages.items)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id', 'doc_count': 1}]}}})
        request_messages = AnalyticComputation.compute_m_from_users(MessageAnalytic(self.time_range, "project", "m:from_users"), self.es)
        self.assertEqual(1, request_messages.count)
        self.assertEqual(['message_id'], request_messages.items)

    def test_compute_m_segmentation(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        message_segmentation = AnalyticComputation.compute_m_segmentation(MessageAnalytic(self.time_range, "project", "m:segmentation"), self.es)
        self.assertEqual([], message_segmentation.counts)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'request', 'doc_count': 1}]}}})
        message_segmentation = AnalyticComputation.compute_m_segmentation(MessageAnalytic(self.time_range, "project", "m:segmentation"), self.es)
        self.assertEqual(1, len(message_segmentation.counts))
        self.assertEqual([Segmentation("request", 1)], message_segmentation.counts)

    def test_compute_r_segmentation(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        request_message_segmentation = AnalyticComputation.compute_r_segmentation(MessageAnalytic(self.time_range, "project", "r:segmentation"), self.es)
        self.assertEqual([], request_message_segmentation.counts)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'text', 'doc_count': 1}]}}})
        request_message_segmentation = AnalyticComputation.compute_r_segmentation(MessageAnalytic(self.time_range, "project", "r:segmentation"), self.es)
        self.assertEqual(1, len(request_message_segmentation.counts))
        self.assertEqual([Segmentation("text", 1)], request_message_segmentation.counts)

    def test_compute_m_from_bot(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        bot_messages = AnalyticComputation.compute_m_from_bot(MessageAnalytic(self.time_range, "project", "m:from_bot"), self.es)
        self.assertEqual(0, bot_messages.count)
        self.assertEqual([], bot_messages.items)

        self.es.search = Mock(side_effect=[
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id', 'doc_count': 1}]}}},
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}}
        ])
        bot_messages = AnalyticComputation.compute_m_from_bot(MessageAnalytic(self.time_range, "project", "m:from_bot"), self.es)
        self.assertEqual(1, bot_messages.count)
        self.assertEqual(['message_id'], bot_messages.items)

        self.es.search = Mock(side_effect=[
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}},
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id', 'doc_count': 1}]}}}
        ])
        bot_messages = AnalyticComputation.compute_m_from_bot(MessageAnalytic(self.time_range, "project", "m:from_bot"), self.es)
        self.assertEqual(1, bot_messages.count)
        self.assertEqual(['message_id'], bot_messages.items)

        self.es.search = Mock(side_effect=[
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id1', 'doc_count': 1}]}}},
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id2', 'doc_count': 1}]}}}
        ])
        bot_messages = AnalyticComputation.compute_m_from_bot(MessageAnalytic(self.time_range, "project", "m:from_bot"), self.es)
        self.assertEqual(2, bot_messages.count)
        self.assertEqual(['message_id1', 'message_id2'], bot_messages.items)

    def test_compute_m_responses(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        response_messages = AnalyticComputation.compute_m_responses(MessageAnalytic(self.time_range, "project", "m:responses"), self.es)
        self.assertEqual(0, response_messages.count)
        self.assertEqual([], response_messages.items)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id', 'doc_count': 1}]}}})
        response_messages = AnalyticComputation.compute_m_responses(MessageAnalytic(self.time_range, "project", "m:responses"), self.es)
        self.assertEqual(1, response_messages.count)
        self.assertEqual(['message_id'], response_messages.items)

    def test_compute_m_notifications(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        notification_messages = AnalyticComputation.compute_m_notifications(MessageAnalytic(self.time_range, "project", "m:notifications"), self.es)
        self.assertEqual(0, notification_messages.count)
        self.assertEqual([], notification_messages.items)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id', 'doc_count': 1}]}}})
        notification_messages = AnalyticComputation.compute_m_notifications(MessageAnalytic(self.time_range, "project", "m:notifications"), self.es)
        self.assertEqual(1, notification_messages.count)
        self.assertEqual(['message_id'], notification_messages.items)

    def test_compute_task_t_total(self):
        self.task_manager_interface.get_all_tasks = Mock(return_value=[])
        total_tasks = AnalyticComputation.compute_task_t_total(TaskAnalytic(self.time_range, "app_id", "t:total"), self.task_manager_interface)
        self.assertEqual(0, total_tasks.count)

        self.task_manager_interface.get_all_tasks = Mock(side_effect=[
            [Task("task_id1", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""))],
            []
        ])
        total_tasks = AnalyticComputation.compute_task_t_total(TaskAnalytic(self.time_range, "app_id", "t:total"), self.task_manager_interface)
        self.assertEqual(1, total_tasks.count)
        self.assertEqual(["task_id1"], total_tasks.items)

        self.task_manager_interface.get_all_tasks = Mock(side_effect=[
            [],
            [Task("task_id2", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""), close_ts=datetime(2021, 6, 12).timestamp())]
        ])
        total_tasks = AnalyticComputation.compute_task_t_total(TaskAnalytic(self.time_range, "app_id", "t:total"), self.task_manager_interface)
        self.assertEqual(1, total_tasks.count)
        self.assertEqual(["task_id2"], total_tasks.items)

    def test_compute_task_t_active(self):
        self.task_manager_interface.get_all_tasks = Mock(return_value=[])
        active_tasks = AnalyticComputation.compute_task_t_active(TaskAnalytic(self.time_range, "app_id", "t:active"), self.task_manager_interface)
        self.assertEqual(0, active_tasks.count)

        self.task_manager_interface.get_all_tasks = Mock(return_value=[Task("task_id", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""))])
        active_tasks = AnalyticComputation.compute_task_t_active(TaskAnalytic(self.time_range, "app_id", "t:active"), self.task_manager_interface)
        self.assertEqual(1, active_tasks.count)
        self.assertEqual(["task_id"], active_tasks.items)

    def test_compute_task_t_closed(self):
        self.task_manager_interface.get_all_tasks = Mock(return_value=[])
        closed_tasks = AnalyticComputation.compute_task_t_closed(TaskAnalytic(self.time_range, "app_id", "t:closed"), self.task_manager_interface)
        self.assertEqual(0, closed_tasks.count)

        self.task_manager_interface.get_all_tasks = Mock(return_value=[Task("task_id", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""), close_ts=datetime(2021, 6, 12).timestamp())])
        closed_tasks = AnalyticComputation.compute_task_t_closed(TaskAnalytic(self.time_range, "app_id", "t:closed"), self.task_manager_interface)
        self.assertEqual(1, closed_tasks.count)
        self.assertEqual(["task_id"], closed_tasks.items)

    def test_compute_task_t_new(self):
        self.task_manager_interface.get_all_tasks = Mock(return_value=[])
        new_tasks = AnalyticComputation.compute_task_t_new(TaskAnalytic(self.time_range, "app_id", "t:new"), self.task_manager_interface)
        self.assertEqual(0, new_tasks.count)

        self.task_manager_interface.get_all_tasks = Mock(side_effect=[
            [Task("task_id", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""))],
        ])
        new_tasks = AnalyticComputation.compute_task_t_new(TaskAnalytic(self.time_range, "app_id", "t:new"), self.task_manager_interface)
        self.assertEqual(1, new_tasks.count)
        self.assertEqual(["task_id"], new_tasks.items)

    def test_compute_transaction_t_total(self):
        self.task_manager_interface.get_all_transactions = Mock(return_value=[])
        total_transactions = AnalyticComputation.compute_transaction_t_total(TransactionAnalytic(self.time_range, "app_id", "t:total"), self.task_manager_interface)
        self.assertEqual(0, total_transactions.count)

        self.task_manager_interface.get_all_transactions = Mock(return_value=[TaskTransaction("transaction_id", "task_id", "label", int(datetime(2021, 6, 12).timestamp()), int(datetime(2021, 6, 12).timestamp()), "actioneer_id", None)])
        total_transactions = AnalyticComputation.compute_transaction_t_total(TransactionAnalytic(self.time_range, "app_id", "t:total"), self.task_manager_interface)
        self.assertEqual(1, total_transactions.count)
        self.assertEqual([TransactionReturn("task_id", ["transaction_id"])], total_transactions.transactions)

    def test_compute_transaction_t_segmentation(self):
        self.task_manager_interface.get_all_transactions = Mock(return_value=[])
        transaction_segmentation = AnalyticComputation.compute_transaction_t_segmentation(TransactionAnalytic(self.time_range, "app_id", "t:segmentation"), self.task_manager_interface)
        self.assertEqual([], transaction_segmentation.counts)

        self.task_manager_interface.get_all_transactions = Mock(return_value=[TaskTransaction("transaction_id", "task_id", "label", int(datetime(2021, 6, 12).timestamp()), int(datetime(2021, 6, 12).timestamp()), "actioneer_id", None)])
        transaction_segmentation = AnalyticComputation.compute_transaction_t_segmentation(TransactionAnalytic(self.time_range, "app_id", "t:segmentation"), self.task_manager_interface)
        self.assertEqual(1, len(transaction_segmentation.counts))
        self.assertEqual([Segmentation("label", 1)], transaction_segmentation.counts)
