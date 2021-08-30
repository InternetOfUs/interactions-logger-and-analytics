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
from datetime import datetime

from mock import Mock

from memex_logging.common.dao.common import DocumentNotFound
from memex_logging.common.model.analytic.analytic import Analytic
from memex_logging.common.model.analytic.descriptor.count import UserCountDescriptor
from memex_logging.common.model.analytic.result.count import CountResult
from test.unit.memex_logging.common_test.common_test_ws import CommonWsTestCase
from test.unit.memex_logging.common_test.generator.time import TimeGenerator


class TestAnalyticInterface(CommonWsTestCase):

    def test_get_analytic(self):
        analytic_id = "analytic_id"
        analytic = Analytic(analytic_id, UserCountDescriptor(TimeGenerator.generate_random(), "project", "new"), CountResult(1, datetime.now(), datetime.now(), datetime.now()))
        self.dao_collector.analytic.get = Mock(return_value=analytic)
        response = self.client.get(f"/analytic?id={analytic_id}")
        self.assertEqual(200, response.status_code)
        self.assertEqual(analytic.to_repr(), json.loads(response.data))

        response = self.client.get("/analytic")
        self.assertEqual(400, response.status_code)

        self.dao_collector.analytic.get = Mock(side_effect=DocumentNotFound())
        response = self.client.get(f"/analytic?id={analytic_id}")
        self.assertEqual(404, response.status_code)

        self.dao_collector.analytic.get = Mock(side_effect=Exception)
        response = self.client.get(f"/analytic?id={analytic_id}")
        self.assertEqual(500, response.status_code)

    def test_post_analytic(self):
        raw_descriptor = UserCountDescriptor(TimeGenerator.generate_random(), "project", "total").to_repr()
        self.dao_collector.analytic.add = Mock(return_value=None)
        response = self.client.post("/analytic", json=raw_descriptor)
        self.assertEqual(200, response.status_code)

        response = self.client.post("/analytic", json={})
        self.assertEqual(400, response.status_code)

        self.dao_collector.analytic.add = Mock(side_effect=Exception)
        response = self.client.post("/analytic", json=raw_descriptor)
        self.assertEqual(500, response.status_code)

    def test_delete_analytic(self):
        analytic_id = "analytic_id"

        self.dao_collector.analytic.delete = Mock(return_value=None)
        response = self.client.delete(f"/analytic?id={analytic_id}")
        self.assertEqual(200, response.status_code)
        self.assertEqual({}, json.loads(response.data))

        response = self.client.delete("/analytic")
        self.assertEqual(400, response.status_code)

        self.dao_collector.analytic.delete = Mock(side_effect=Exception)
        response = self.client.delete(f"/analytic?id={analytic_id}")
        self.assertEqual(500, response.status_code)
