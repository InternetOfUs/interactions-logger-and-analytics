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
from freezegun import freeze_time
from wenet.interface.client import ApikeyClient
from wenet.interface.wenet import WeNet
from wenet.model.task.task import Task, TaskGoal
from wenet.model.task.transaction import TaskTransaction
from wenet.model.user.common import Date, Gender
from wenet.model.user.profile import WeNetUserProfile

from memex_logging.common.computation.analytic import AnalyticComputation
from memex_logging.common.model.analytic.descriptor.count import UserCountDescriptor, MessageCountDescriptor, TaskCountDescriptor, TransactionCountDescriptor
from memex_logging.common.model.analytic.descriptor.segmentation import UserSegmentationDescriptor, \
    MessageSegmentationDescriptor, TransactionSegmentationDescriptor
from memex_logging.common.model.analytic.result.count import CountResult
from memex_logging.common.model.analytic.result.segmentation import SegmentationResult, Segmentation
from memex_logging.common.model.time import FixedTimeWindow


class TestAnalyticComputation(TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.es = Elasticsearch()
        self.wenet_interface = WeNet.build(ApikeyClient("apikey"), platform_url="platform_url")
        self.analytic_computation = AnalyticComputation(self.es, self.wenet_interface)
        self.time_range = FixedTimeWindow(datetime(2021, 1, 1), datetime(2021, 12, 31))

    def test_compute_total_users(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}, 'type_count': {'value': 0}}})
        total_users = self.analytic_computation.get_result(UserCountDescriptor(self.time_range, "project", "total"))
        self.assertIsInstance(total_users, CountResult)
        self.assertEqual(0, total_users.count)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'user-id-1', 'doc_count': 1}]}, 'type_count': {'value': 1}}})
        total_users = self.analytic_computation.get_result(UserCountDescriptor(self.time_range, "project", "total"))
        self.assertIsInstance(total_users, CountResult)
        self.assertEqual(1, total_users.count)

    def test_compute_active_users(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}, 'type_count': {'value': 0}}})
        active_users = self.analytic_computation.get_result(UserCountDescriptor(self.time_range, "project", "active"))
        self.assertIsInstance(active_users, CountResult)
        self.assertEqual(0, active_users.count)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'user-id-1', 'doc_count': 1}]}, 'type_count': {'value': 1}}})
        active_users = self.analytic_computation.get_result(UserCountDescriptor(self.time_range, "project", "active"))
        self.assertIsInstance(active_users, CountResult)
        self.assertEqual(1, active_users.count)

    def test_compute_engaged_users(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}, 'type_count': {'value': 0}}})
        engaged_users = self.analytic_computation.get_result(UserCountDescriptor(self.time_range, "project", "engaged"))
        self.assertIsInstance(engaged_users, CountResult)
        self.assertEqual(0, engaged_users.count)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'user-id-1', 'doc_count': 1}]}, 'type_count': {'value': 1}}})
        engaged_users = self.analytic_computation.get_result(UserCountDescriptor(self.time_range, "project", "engaged"))
        self.assertIsInstance(engaged_users, CountResult)
        self.assertEqual(1, engaged_users.count)

    def test_compute_new_users(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}, 'type_count': {'value': 0}}})
        new_users = self.analytic_computation.get_result(UserCountDescriptor(self.time_range, "project", "new"))
        self.assertIsInstance(new_users, CountResult)
        self.assertEqual(0, new_users.count)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'user-id-1', 'doc_count': 1}]}, 'type_count': {'value': 1}}})
        new_users = self.analytic_computation.get_result(UserCountDescriptor(self.time_range, "project", "new"))
        self.assertIsInstance(new_users, CountResult)
        self.assertEqual(0, new_users.count)

        self.es.search = Mock(side_effect=[
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'user-id-1', 'doc_count': 1}]}, 'type_count': {'value': 1}}},
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}, 'type_count': {'value': 0}}},
        ])
        new_users = self.analytic_computation.get_result(UserCountDescriptor(self.time_range, "project", "new"))
        self.assertIsInstance(new_users, CountResult)
        self.assertEqual(1, new_users.count)

    def test_compute_user_age_segmentation(self):
        self.wenet_interface.hub.get_user_ids_for_app = Mock(return_value=[])
        age_segmentation = self.analytic_computation.get_result(UserSegmentationDescriptor(self.time_range, "project", "age"))
        self.assertIsInstance(age_segmentation, SegmentationResult)
        self.assertEqual([
            Segmentation("0-18", 0),
            Segmentation("19-25", 0),
            Segmentation("26-35", 0),
            Segmentation("36-45", 0),
            Segmentation("46-55", 0),
            Segmentation("55+", 0),
            Segmentation("unavailable", 0)
        ], age_segmentation.segments)

        self.wenet_interface.hub.get_user_ids_for_app = Mock(return_value=["1", "2", "3", "4", "5", "6", "7"])
        self.wenet_interface.profile_manager.get_user_profile = Mock(side_effect=[
            WeNetUserProfile(None, Date(2011, 3, 10), None, None, None, None, None, None, None, None, None, "1", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, Date(2001, 3, 10), None, None, None, None, None, None, None, None, None, "2", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, Date(1991, 3, 10), None, None, None, None, None, None, None, None, None, "3", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, Date(1981, 3, 10), None, None, None, None, None, None, None, None, None, "4", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, Date(1971, 8, 20), None, None, None, None, None, None, None, None, None, "5", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, Date(1961, 11, 30), None, None, None, None, None, None, None, None, None, "6", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, None, None, None, None, None, None, None, None, None, None, "7", None, None, None, None, None, None, None, None),
        ])
        with freeze_time("2021-07-31"):
            age_segmentation = self.analytic_computation.get_result(UserSegmentationDescriptor(self.time_range, "project", "age"))
            self.assertIsInstance(age_segmentation, SegmentationResult)
            self.assertEqual([
                Segmentation("0-18", 1),
                Segmentation("19-25", 1),
                Segmentation("26-35", 1),
                Segmentation("36-45", 1),
                Segmentation("46-55", 1),
                Segmentation("55+", 1),
                Segmentation("unavailable", 1)
            ], age_segmentation.segments)

    def test_compute_user_gender_segmentation(self):
        self.wenet_interface.hub.get_user_ids_for_app = Mock(return_value=[])
        gender_segmentation = self.analytic_computation.get_result(UserSegmentationDescriptor(self.time_range, "project", "gender"))
        self.assertIsInstance(gender_segmentation, SegmentationResult)
        self.assertEqual([
            Segmentation("male", 0),
            Segmentation("female", 0),
            Segmentation("non-binary", 0),
            Segmentation("in-another-way", 0),
            Segmentation("prefer-not-to-say", 0),
            Segmentation("unavailable", 0)
        ], gender_segmentation.segments)

        self.wenet_interface.hub.get_user_ids_for_app = Mock(return_value=["1", "2", "3", "4", "5", "6"])
        self.wenet_interface.profile_manager.get_user_profile = Mock(side_effect=[
            WeNetUserProfile(None, None, Gender.MALE, None, None, None, None, None, None, None, None, "1", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, None, Gender.FEMALE, None, None, None, None, None, None, None, None, "2", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, None, Gender.NOT_SAY, None, None, None, None, None, None, None, None, "3", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, None, Gender.OTHER, None, None, None, None, None, None, None, None, "4", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, None, Gender.NON_BINARY, None, None, None, None, None, None, None, None, "5", None, None, None, None, None, None, None, None),
            WeNetUserProfile(None, None, None, None, None, None, None, None, None, None, None, "6", None, None, None, None, None, None, None, None),
        ])
        gender_segmentation = self.analytic_computation.get_result(UserSegmentationDescriptor(self.time_range, "project", "gender"))
        self.assertIsInstance(gender_segmentation, SegmentationResult)
        self.assertEqual([
            Segmentation("male", 1),
            Segmentation("female", 1),
            Segmentation("non-binary", 1),
            Segmentation("in-another-way", 1),
            Segmentation("prefer-not-to-say", 1),
            Segmentation("unavailable", 1)
        ], gender_segmentation.segments)

    def test_compute_user_messages(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        request_messages = self.analytic_computation.get_result(MessageCountDescriptor(self.time_range, "project", "from_users"))
        self.assertIsInstance(request_messages, CountResult)
        self.assertEqual(0, request_messages.count)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id', 'doc_count': 1}]}}})
        request_messages = self.analytic_computation.get_result(MessageCountDescriptor(self.time_range, "project", "from_users"))
        self.assertIsInstance(request_messages, CountResult)
        self.assertEqual(1, request_messages.count)

    def test_compute_messages_segmentation(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        message_segmentation = self.analytic_computation.get_result(MessageSegmentationDescriptor(self.time_range, "project", "all"))
        self.assertIsInstance(message_segmentation, SegmentationResult)
        self.assertEqual([], message_segmentation.segments)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'request', 'doc_count': 1}]}}})
        message_segmentation = self.analytic_computation.get_result(MessageSegmentationDescriptor(self.time_range, "project", "all"))
        self.assertIsInstance(message_segmentation, SegmentationResult)
        self.assertEqual(1, len(message_segmentation.segments))
        self.assertEqual([Segmentation("request", 1)], message_segmentation.segments)

    def test_compute_user_messages_segmentation(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        request_message_segmentation = self.analytic_computation.get_result(MessageSegmentationDescriptor(self.time_range, "project", "from_users"))
        self.assertIsInstance(request_message_segmentation, SegmentationResult)
        self.assertEqual([], request_message_segmentation.segments)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'text', 'doc_count': 1}]}}})
        request_message_segmentation = self.analytic_computation.get_result(MessageSegmentationDescriptor(self.time_range, "project", "from_users"))
        self.assertIsInstance(request_message_segmentation, SegmentationResult)
        self.assertEqual(1, len(request_message_segmentation.segments))
        self.assertEqual([Segmentation("text", 1)], request_message_segmentation.segments)

    def test_compute_bot_messages(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        bot_messages = self.analytic_computation.get_result(MessageCountDescriptor(self.time_range, "project", "from_bot"))
        self.assertIsInstance(bot_messages, CountResult)
        self.assertEqual(0, bot_messages.count)

        self.es.search = Mock(side_effect=[
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id', 'doc_count': 1}]}}},
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}}
        ])
        bot_messages = self.analytic_computation.get_result(MessageCountDescriptor(self.time_range, "project", "from_bot"), )
        self.assertIsInstance(bot_messages, CountResult)
        self.assertEqual(1, bot_messages.count)

        self.es.search = Mock(side_effect=[
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}},
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id', 'doc_count': 1}]}}}
        ])
        bot_messages = self.analytic_computation.get_result(MessageCountDescriptor(self.time_range, "project", "from_bot"))
        self.assertIsInstance(bot_messages, CountResult)
        self.assertEqual(1, bot_messages.count)

        self.es.search = Mock(side_effect=[
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id1', 'doc_count': 1}]}}},
            {'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id2', 'doc_count': 1}]}}}
        ])
        bot_messages = self.analytic_computation.get_result(MessageCountDescriptor(self.time_range, "project", "from_bot"))
        self.assertIsInstance(bot_messages, CountResult)
        self.assertEqual(2, bot_messages.count)

    def test_compute_response_messages(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        response_messages = self.analytic_computation.get_result(MessageCountDescriptor(self.time_range, "project", "responses"))
        self.assertIsInstance(response_messages, CountResult)
        self.assertEqual(0, response_messages.count)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id', 'doc_count': 1}]}}})
        response_messages = self.analytic_computation.get_result(MessageCountDescriptor(self.time_range, "project", "responses"))
        self.assertIsInstance(response_messages, CountResult)
        self.assertEqual(1, response_messages.count)

    def test_compute_notification_messages(self):
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': []}}})
        notification_messages = self.analytic_computation.get_result(MessageCountDescriptor(self.time_range, "project", "notifications"))
        self.assertIsInstance(notification_messages, CountResult)
        self.assertEqual(0, notification_messages.count)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': []}, 'aggregations': {'terms_count': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 'message_id', 'doc_count': 1}]}}})
        notification_messages = self.analytic_computation.get_result(MessageCountDescriptor(self.time_range, "project", "notifications"))
        self.assertIsInstance(notification_messages, CountResult)
        self.assertEqual(1, notification_messages.count)

    def test_compute_total_tasks(self):
        self.wenet_interface.task_manager.get_all_tasks = Mock(return_value=[])
        total_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "total"))
        self.assertIsInstance(total_tasks, CountResult)
        self.assertEqual(0, total_tasks.count)

        self.wenet_interface.task_manager.get_all_tasks = Mock(side_effect=[
            [Task("task_id1", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""))],
            [],
            []
        ])
        total_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "total"))
        self.assertIsInstance(total_tasks, CountResult)
        self.assertEqual(1, total_tasks.count)

        self.wenet_interface.task_manager.get_all_tasks = Mock(side_effect=[
            [],
            [Task("task_id2", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""), close_ts=datetime(2022, 1, 1).timestamp())],
            []
        ])
        total_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "total"))
        self.assertIsInstance(total_tasks, CountResult)
        self.assertEqual(1, total_tasks.count)

        self.wenet_interface.task_manager.get_all_tasks = Mock(side_effect=[
            [],
            [],
            [Task("task_id3", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""), close_ts=datetime(2021, 6, 12).timestamp())]
        ])
        total_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "total"))
        self.assertIsInstance(total_tasks, CountResult)
        self.assertEqual(1, total_tasks.count)

        self.wenet_interface.task_manager.get_all_tasks = Mock(side_effect=[
            [Task("task_id1", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""))],
            [Task("task_id2", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""), close_ts=datetime(2022, 1, 1).timestamp())],
            [Task("task_id3", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""), close_ts=datetime(2021, 6, 12).timestamp())]
        ])
        total_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "total"))
        self.assertIsInstance(total_tasks, CountResult)
        self.assertEqual(3, total_tasks.count)

    def test_compute_active_tasks(self):
        self.wenet_interface.task_manager.get_all_tasks = Mock(return_value=[])
        active_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "active"))
        self.assertIsInstance(active_tasks, CountResult)
        self.assertEqual(0, active_tasks.count)

        self.wenet_interface.task_manager.get_all_tasks = Mock(side_effect=[
            [Task("task_id1", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""))],
            []
        ])
        active_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "active"))
        self.assertIsInstance(active_tasks, CountResult)
        self.assertEqual(1, active_tasks.count)

        self.wenet_interface.task_manager.get_all_tasks = Mock(side_effect=[
            [],
            [Task("task_id2", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""), close_ts=datetime(2022, 1, 1).timestamp())]
        ])
        active_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "active"))
        self.assertIsInstance(active_tasks, CountResult)
        self.assertEqual(1, active_tasks.count)

        self.wenet_interface.task_manager.get_all_tasks = Mock(side_effect=[
            [Task("task_id1", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""))],
            [Task("task_id2", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""), close_ts=datetime(2022, 1, 1).timestamp())]
        ])
        active_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "active"))
        self.assertIsInstance(active_tasks, CountResult)
        self.assertEqual(2, active_tasks.count)

    def test_compute_closed_tasks(self):
        self.wenet_interface.task_manager.get_all_tasks = Mock(return_value=[])
        closed_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "closed"))
        self.assertIsInstance(closed_tasks, CountResult)
        self.assertEqual(0, closed_tasks.count)

        self.wenet_interface.task_manager.get_all_tasks = Mock(return_value=[Task("task_id", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""), close_ts=datetime(2021, 6, 12).timestamp())])
        closed_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "closed"))
        self.assertIsInstance(closed_tasks, CountResult)
        self.assertEqual(1, closed_tasks.count)

    def test_compute_new_tasks(self):
        self.wenet_interface.task_manager.get_all_tasks = Mock(return_value=[])
        new_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "new"))
        self.assertIsInstance(new_tasks, CountResult)
        self.assertEqual(0, new_tasks.count)

        self.wenet_interface.task_manager.get_all_tasks = Mock(side_effect=[
            [Task("task_id", datetime(2021, 6, 12).timestamp(), datetime(2021, 6, 12).timestamp(), "ask4help", "requester_id", "app_id", None, TaskGoal("goal", ""))],
        ])
        new_tasks = self.analytic_computation.get_result(TaskCountDescriptor(self.time_range, "app_id", "new"))
        self.assertIsInstance(new_tasks, CountResult)
        self.assertEqual(1, new_tasks.count)

    def test_compute_total_transactions(self):
        self.wenet_interface.task_manager.get_all_transactions = Mock(return_value=[])
        total_transactions = self.analytic_computation.get_result(TransactionCountDescriptor(self.time_range, "app_id", "total"))
        self.assertIsInstance(total_transactions, CountResult)
        self.assertEqual(0, total_transactions.count)

        self.wenet_interface.task_manager.get_all_transactions = Mock(return_value=[TaskTransaction("transaction_id", "task_id", "label", int(datetime(2021, 6, 12).timestamp()), int(datetime(2021, 6, 12).timestamp()), "actioneer_id", None)])
        total_transactions = self.analytic_computation.get_result(TransactionCountDescriptor(self.time_range, "app_id", "total"))
        self.assertIsInstance(total_transactions, CountResult)
        self.assertEqual(1, total_transactions.count)

    def test_compute_transactions_segmentation(self):
        self.wenet_interface.task_manager.get_all_transactions = Mock(return_value=[])
        transaction_segmentation = self.analytic_computation.get_result(TransactionSegmentationDescriptor(self.time_range, "app_id", "label"))
        self.assertIsInstance(transaction_segmentation, SegmentationResult)
        self.assertEqual([], transaction_segmentation.segments)

        self.wenet_interface.task_manager.get_all_transactions = Mock(return_value=[TaskTransaction("transaction_id", "task_id", "label", int(datetime(2021, 6, 12).timestamp()), int(datetime(2021, 6, 12).timestamp()), "actioneer_id", None)])
        transaction_segmentation = self.analytic_computation.get_result(TransactionSegmentationDescriptor(self.time_range, "app_id", "label"))
        self.assertIsInstance(transaction_segmentation, SegmentationResult)
        self.assertEqual(1, len(transaction_segmentation.segments))
        self.assertEqual([Segmentation("label", 1)], transaction_segmentation.segments)
