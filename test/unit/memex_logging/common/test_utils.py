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

from freezegun import freeze_time

from memex_logging.common.model.time import FixedTimeWindow, MovingTimeWindow
from memex_logging.common.utils import Utils


class TestUtils(TestCase):

    def test_generate_index(self):
        index = Utils.generate_index("data_type")
        self.assertEqual("data_type-*", index)

        index = Utils.generate_index("data_type", project="project")
        self.assertEqual("data_type-project-*", index)

        with self.assertRaises(ValueError):
            Utils.generate_index("data_type", dt=datetime(2021, 2, 5))

        index = Utils.generate_index("data_type", project="project", dt=datetime(2021, 2, 5))
        self.assertEqual("data_type-project-2021-02-05", index)

    def test_extract_range_timestamps(self):
        start = datetime(2021, 7, 24)
        end = datetime(2021, 7, 28)
        self.assertEqual((start, end), Utils.extract_range_timestamps(FixedTimeWindow(start, end)))

        with freeze_time("2021-07-31"):
            self.assertEqual((datetime(2021, 7, 28), datetime(2021, 7, 31)), Utils.extract_range_timestamps(MovingTimeWindow("3D")))
            self.assertEqual((datetime(2021, 7, 24), datetime(2021, 7, 31)), Utils.extract_range_timestamps(MovingTimeWindow("1W")))
            self.assertEqual((datetime(2020, 9, 30), datetime(2021, 7, 31)), Utils.extract_range_timestamps(MovingTimeWindow("10M")))
            self.assertEqual((datetime(2019, 7, 31), datetime(2021, 7, 31)), Utils.extract_range_timestamps(MovingTimeWindow("2Y")))

    def test_compute_age(self):
        with freeze_time("2021-07-31"):
            self.assertEqual(0, Utils.compute_age(datetime(2021, 7, 31)))
            self.assertEqual(1, Utils.compute_age(datetime(2020, 7, 31)))
            self.assertEqual(40, Utils.compute_age(datetime(1981, 3, 10)))
            self.assertEqual(39, Utils.compute_age(datetime(1981, 8, 30)))
