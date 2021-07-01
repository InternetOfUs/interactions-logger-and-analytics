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

from memex_logging.common.model.time import MovingTimeWindow, FixedTimeWindow


class TestMovingTimeWindow(TestCase):

    def test_repr(self):
        time = MovingTimeWindow("7D")
        self.assertEqual(time, MovingTimeWindow.from_repr(time.to_repr()))


class TestFixedTimeWindow(TestCase):

    def test_repr(self):
        time = FixedTimeWindow(datetime.now(), datetime.now())
        self.assertEqual(time, FixedTimeWindow.from_repr(time.to_repr()))
