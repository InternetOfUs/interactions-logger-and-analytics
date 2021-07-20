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

import json

from mock import Mock

from test.unit.memex_logging.ws.common.common_test_ws import CommonWsTestCase


class TestAnalyticInterface(CommonWsTestCase):

    def test_get_analytic(self):
        project = "project"
        static_id = "static_id"
        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 2, 'successful': 2, 'skipped': 0, 'failed': 0}, 'hits': {
            'total': {'value': 1, 'relation': 'eq'}, 'max_score': 0.6931472, 'hits': [{'_index': 'analytic-i2afrcoxx3-task', '_type': '_doc', '_id': '24xhqXoBLjue6xXvxUTC', '_score': 0.6931472, '_source': {
                'query': {'timespan': {'type': 'MOVING', 'value': '30D'}, 'project': 'project', 'type': 'analytic', 'dimension': 'task', 'metric': 't:new'},
                'result': {'count': 1, 'items': ['60e310c46fba621ff95b5b69'], 'type': 'taskId', 'creationDt': '2021-07-15T11:00:01.065638', 'fromDt': '2021-06-15T00:00:00', 'toDt': '2021-07-15T00:00:00'},
                'staticId': 'static_id'}}]}})
        response = self.client.get(f"/analytic?staticId={static_id}&project={project}")
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            'query': {'timespan': {'type': 'MOVING', 'value': '30D'}, 'project': 'project', 'type': 'analytic', 'dimension': 'task', 'metric': 't:new'},
            'result': {'count': 1, 'items': ['60e310c46fba621ff95b5b69'], 'type': 'taskId', 'creationDt': '2021-07-15T11:00:01.065638', 'fromDt': '2021-06-15T00:00:00', 'toDt': '2021-07-15T00:00:00'},
            'staticId': 'static_id'
        }, json.loads(response.data))

        response = self.client.get("/analytic")
        self.assertEqual(400, response.status_code)

        self.es.search = Mock(return_value={'took': 1, 'timed_out': False, '_shards': {'total': 2, 'successful': 2, 'skipped': 0, 'failed': 0}, 'hits': {
            'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}})
        response = self.client.get(f"/analytic?staticId={static_id}&project={project}")
        self.assertEqual(404, response.status_code)

        self.es.search = Mock(side_effect=Exception)
        response = self.client.get(f"/analytic?staticId={static_id}&project={project}")
        self.assertEqual(500, response.status_code)

    def test_delete_analytic(self):
        project = "project"
        static_id = "static_id"

        self.es.delete_by_query = Mock(return_value=None)
        response = self.client.delete(f"/analytic?staticId={static_id}&project={project}")
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            "status": "Ok: analytic deleted",
            "code": 200
        }, json.loads(response.data))

        response = self.client.delete("/analytic")
        self.assertEqual(400, response.status_code)

        self.es.delete_by_query = Mock(side_effect=Exception)
        response = self.client.delete(f"/analytic?staticId={static_id}&project={project}")
        self.assertEqual(500, response.status_code)
