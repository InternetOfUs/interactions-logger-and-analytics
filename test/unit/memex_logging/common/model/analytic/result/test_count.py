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

from memex_logging.common.model.analytic.result.builder import AnalyticResultBuilder
from memex_logging.common.model.analytic.result.count import CountResult


class TestCountResult(TestCase):

    def test_repr(self):
        result = CountResult(1, datetime.now(), datetime.now(), datetime.now())
        self.assertEqual(result, CountResult.from_repr(result.to_repr()))
        self.assertEqual(result, AnalyticResultBuilder.build(result.to_repr()))
