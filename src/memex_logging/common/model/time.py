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


class MovingTimeWindow:

    MOVING_TIME_WINDOW_TYPE = "MOVING"
    ALLOWED_MOVING_TIME_WINDOW_VALUES = ["30D", "10D", "7D", "1D", "TODAY"]

    def __init__(self, value: str):
        self.value = value

    def to_repr(self) -> dict:
        return{
            'type': self.MOVING_TIME_WINDOW_TYPE,
            'value': self.value.upper()
        }

    @staticmethod
    def from_repr(raw_data: dict) -> MovingTimeWindow:
        if str(raw_data['type']).upper() != MovingTimeWindow.MOVING_TIME_WINDOW_TYPE:
            raise ValueError("Unrecognized type for MovingTimeWindow")

        if str(raw_data['value']).upper() not in MovingTimeWindow.ALLOWED_MOVING_TIME_WINDOW_VALUES:
            raise ValueError('Unknown value for value in the MovingTimeWindow')

        return MovingTimeWindow(raw_data['value'])

    def __eq__(self, o) -> bool:
        if isinstance(o, MovingTimeWindow):
            return o.value == self.value
        else:
            return False


class FixedTimeWindow:

    FIXED_TIME_WINDOW_TYPE = "FIXED"

    def __init__(self, start: datetime, end: datetime):
        self.start = start
        self.end = end

    def to_repr(self) -> dict:
        return{
            'type': self.FIXED_TIME_WINDOW_TYPE,
            'start': self.start.isoformat(),
            'end': self.end.isoformat()
        }

    @staticmethod
    def from_isoformat(start: str, end: str) -> FixedTimeWindow:
        return FixedTimeWindow(datetime.fromisoformat(start), datetime.fromisoformat(end))

    @staticmethod
    def from_repr(raw_data: dict) -> FixedTimeWindow:
        if str(raw_data['type']).upper() != FixedTimeWindow.FIXED_TIME_WINDOW_TYPE:
            raise ValueError("Unrecognized type for FixedTimeWindow")

        return FixedTimeWindow.from_isoformat(raw_data['start'], raw_data['end'])

    def __eq__(self, o) -> bool:
        if isinstance(o, FixedTimeWindow):
            return o.start == self.start and o.end == self.end
        else:
            return False
