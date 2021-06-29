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

from memex_logging.common.model.time import DefaultTime, CustomTime


class TestDefaultTime(TestCase):

    def test_repr(self):
        time = DefaultTime("7D")
        self.assertEqual(time, DefaultTime.from_repr(time.to_repr()))


class TestCustomTime(TestCase):

    def test_repr(self):
        time = CustomTime(datetime.now(), datetime.now())
        self.assertEqual(time, CustomTime.from_repr(time.to_repr()))
