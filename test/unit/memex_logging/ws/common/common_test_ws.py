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

from memex_logging.ws.ws import WsInterface
from test.unit.memex_logging.ws.common.mock.daos import MockDaoCollectorBuilder


class CommonWsTestCase(TestCase):
    """
    A common test case for the smart-places web service resources
    """

    def setUp(self) -> None:
        super().setUp()
        self.dao_collector = MockDaoCollectorBuilder.build_mock_daos()
        api = WsInterface(self.dao_collector, None)
        api.get_application().testing = True
        self.client = api.get_application().test_client()
