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

from memex_logging.common.model.time import MovingTimeWindow, FixedTimeWindow, TimeWindow


class TestMovingTimeWindow(TestCase):

    def test_repr(self):
        time_window = MovingTimeWindow("7D")
        self.assertEqual(time_window, MovingTimeWindow.from_repr(time_window.to_repr()))
        self.assertEqual(time_window, TimeWindow.from_repr(time_window.to_repr()))

    def test_from_deprecated_repr(self):
        raw_time = {
            'type': "DEFAULT",
            'value': "30D"
        }
        self.assertIsInstance(MovingTimeWindow.from_repr(raw_time), MovingTimeWindow)
        self.assertIsInstance(TimeWindow.from_repr(raw_time), MovingTimeWindow)

    def test_incorrect_creation(self):
        with self.assertRaises(ValueError):
            MovingTimeWindow("30GG")


class TestFixedTimeWindow(TestCase):

    def test_repr(self):
        time = FixedTimeWindow(datetime.now(), datetime.now())
        self.assertEqual(time, FixedTimeWindow.from_repr(time.to_repr()))
        self.assertEqual(time, TimeWindow.from_repr(time.to_repr()))

    def test_from_deprecated_repr(self):
        raw_time = {
            'type': "CUSTOM",
            'start': datetime.now().isoformat(),
            'end': datetime.now().isoformat()
        }
        self.assertIsInstance(FixedTimeWindow.from_repr(raw_time), FixedTimeWindow)
        self.assertIsInstance(TimeWindow.from_repr(raw_time), FixedTimeWindow)
