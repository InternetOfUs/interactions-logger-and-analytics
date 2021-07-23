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

from unittest import TestCase
from unittest.mock import Mock

from elasticsearch import Elasticsearch
from freezegun import freeze_time

from memex_logging.migration.actions.analytic_reorganization_migration import AnalyticReorganizationMigration


class TestAnalyticComputation(TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.es = Elasticsearch()
        self.es.delete = Mock()
        self.es.index = Mock()
        self.es.scroll = Mock(return_value={})
        self.migration = AnalyticReorganizationMigration()

    def test_apply_no_data_changed(self):
        self.es.search = Mock(return_value={'_scroll_id': 'scroll_id', 'took': 6, 'timed_out': False, '_shards': {'total': 14, 'successful': 14, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': [{
            '_index': 'analytic-i2afrcoxx3-message', '_type': '_doc', '_id': 'pMuWzXoBKDx30EUdrHBt', '_score': None, '_source': {
                'id': 'c48d5f6b-695e-4f34-9ba8-045ce2f68bb5',
                'descriptor': {'timespan': {'type': 'MOVING', 'value': '30d'}, 'project': 'I2AFRCOXx3', 'type': 'count', 'dimension': 'message', 'metric': 'from_users'},
                'result': {'count': 0, 'type': 'count', 'creationDt': '2021-07-23T09:59:44.355955', 'fromDt': '2021-06-23T00:00:00', 'toDt': '2021-07-23T00:00:00'}
            }, 'sort': [0]}]}})
        self.migration.apply(self.es)
        self.es.index.assert_not_called()
        self.es.delete.assert_not_called()

    def test_apply_deleted(self):
        self.es.search = Mock(return_value={'_scroll_id': 'scroll_id', 'took': 6, 'timed_out': False, '_shards': {'total': 14, 'successful': 14, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': [{
            '_index': 'analytic-i2afrcoxx3-message', '_type': '_doc', '_id': 'pMuWzXoBKDx30EUdrHBt', '_score': None, '_source': {
                'id': 'c48d5f6b-695e-4f34-9ba8-045ce2f68bb5',
                'descriptor': {'timespan': {'type': 'MOVING', 'value': '30d'}, 'project': 'I2AFRCOXx3', 'type': 'analytic', 'dimension': 'message', 'metric': 'c:path'},
                'result': None
            }, 'sort': [0]}]}})
        self.migration.apply(self.es)
        self.es.index.assert_not_called()
        self.es.delete.assert_called()

    def test_apply_count(self):
        self.es.search = Mock(return_value={'_scroll_id': 'scroll_id', 'took': 6, 'timed_out': False, '_shards': {'total': 14, 'successful': 14, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': [{
            '_index': 'analytic-i2afrcoxx3-message', '_type': '_doc', '_id': 'pMuWzXoBKDx30EUdrHBt', '_score': None, '_source': {
                'staticId': 'c48d5f6b-695e-4f34-9ba8-045ce2f68bb5',
                'query': {'timespan': {'type': 'MOVING', 'value': '30d'}, 'project': 'I2AFRCOXx3', 'type': 'analytic', 'dimension': 'message', 'metric': 'm:from_users'},
                'result': {'count': 0, 'type': 'messageId', 'items': [], 'creationDt': '2021-07-23T09:59:44.355955', 'fromDt': '2021-06-23T00:00:00', 'toDt': '2021-07-23T00:00:00'}
            }, 'sort': [0]}]}})
        self.migration.apply(self.es)
        self.es.index.assert_called()
        self.assertEqual({
            'id': 'c48d5f6b-695e-4f34-9ba8-045ce2f68bb5',
            'descriptor': {'timespan': {'type': 'MOVING', 'value': '30d'}, 'project': 'I2AFRCOXx3', 'type': 'count', 'dimension': 'message', 'metric': 'from_users'},
            'result': {'count': 0, 'type': 'count', 'creationDt': '2021-07-23T09:59:44.355955', 'fromDt': '2021-06-23T00:00:00', 'toDt': '2021-07-23T00:00:00'}
        }, self.es.index.call_args.kwargs.get('body'))
        self.es.delete.assert_not_called()

    def test_apply_segmentation(self):
        self.es.search = Mock(return_value={'_scroll_id': 'scroll_id', 'took': 6, 'timed_out': False, '_shards': {'total': 14, 'successful': 14, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': [{
            '_index': 'analytic-i2afrcoxx3-message', '_type': '_doc', '_id': 'pMuWzXoBKDx30EUdrHBt', '_score': None, '_source': {
                'id': 'c48d5f6b-695e-4f34-9ba8-045ce2f68bb5',
                'descriptor': {'timespan': {'type': 'MOVING', 'value': '30d'}, 'project': 'I2AFRCOXx3', 'type': 'analytic', 'dimension': 'message', 'metric': 'm:segmentation'},
                'result': {'counts': {'request': 1}}
            }, 'sort': [0]}]}})
        with freeze_time("2021-07-31"):
            self.migration.apply(self.es)
            self.es.index.assert_called()
            self.assertEqual({
                'id': 'c48d5f6b-695e-4f34-9ba8-045ce2f68bb5',
                'descriptor': {'timespan': {'type': 'MOVING', 'value': '30d'}, 'project': 'I2AFRCOXx3', 'type': 'segmentation', 'dimension': 'message', 'metric': 'all'},
                'result': {'segments': [{'count': 1, 'type': 'request'}], 'type': 'segmentation', 'creationDt': '2021-07-31T00:00:00', 'fromDt': '2021-07-31T00:00:00', 'toDt': '2021-07-31T00:00:00'}
            }, self.es.index.call_args.kwargs.get('body'))
            self.es.delete.assert_not_called()

    def test_apply_aggregation(self):
        self.es.search = Mock(return_value={'_scroll_id': 'scroll_id', 'took': 6, 'timed_out': False, '_shards': {'total': 14, 'successful': 14, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': None, 'hits': [{
            '_index': 'analytic-i2afrcoxx3-message', '_type': '_doc', '_id': 'pMuWzXoBKDx30EUdrHBt', '_score': None, '_source': {
                'id': 'c48d5f6b-695e-4f34-9ba8-045ce2f68bb5',
                'descriptor': {'timespan': {'type': 'FIXED', 'start': '2021-07-29T00:00:00', 'end': '2021-07-30T00:00:00'}, 'project': 'I2AFRCOXx3', 'type': 'aggregation', 'field': 'intent.confidence', 'aggregation': 'min'},
                'result': {'min': 0}
            }, 'sort': [0]}]}})
        with freeze_time("2021-07-31"):
            self.migration.apply(self.es)
            self.es.index.assert_called()
            self.assertEqual({
                'id': 'c48d5f6b-695e-4f34-9ba8-045ce2f68bb5',
                'descriptor': {'timespan': {'type': 'FIXED', 'start': '2021-07-29T00:00:00', 'end': '2021-07-30T00:00:00'}, 'project': 'I2AFRCOXx3', 'type': 'aggregation', 'field': 'intent.confidence', 'aggregation': 'min'},
                'result': {'aggregation': 0, 'type': 'aggregation', 'creationDt': '2021-07-31T00:00:00', 'fromDt': '2021-07-29T00:00:00', 'toDt': '2021-07-30T00:00:00'}
            }, self.es.index.call_args.kwargs.get('body'))
            self.es.delete.assert_not_called()
