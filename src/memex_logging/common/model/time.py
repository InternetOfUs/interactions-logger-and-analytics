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

import re
from abc import ABC, abstractmethod
from datetime import datetime

import deprecation


class TimeWindow(ABC):

    @abstractmethod
    def to_repr(self) -> dict:
        pass

    @staticmethod
    @abstractmethod
    def from_repr(raw_data: dict) -> TimeWindow:
        time_window_type = raw_data['type'].upper()
        if time_window_type == MovingTimeWindow.moving_time_window_type():
            timespan = MovingTimeWindow.from_repr(raw_data)
        elif time_window_type == FixedTimeWindow.fixed_time_window_type():
            timespan = FixedTimeWindow.from_repr(raw_data)
        elif time_window_type == MovingTimeWindow.default_time_window_type():
            timespan = MovingTimeWindow.from_repr(raw_data)
        elif time_window_type == FixedTimeWindow.custom_time_window_type():
            timespan = FixedTimeWindow.from_repr(raw_data)
        else:
            raise ValueError(f"Unrecognized type [{time_window_type}] for TimeWindow")

        return timespan


class MovingTimeWindow(TimeWindow):

    def __init__(self, value: str):
        match = re.match(r"^([0-9]+)([dDwWmMyY])$", value)
        if match is not None:
            self.value = int(match.group(1))
            self.descriptor = match.group(2)
        else:
            if value.upper() == "TODAY":
                self.value = None
                self.descriptor = "TODAY"
            else:
                raise ValueError("Incorrect format for MovingTimeWindow value")

    @staticmethod
    def moving_time_window_type():
        return "MOVING"

    @staticmethod
    @deprecation.deprecated(deprecated_in="1.5.0", removed_in="2.0.0", details="Use the moving_time_window_type method instead")
    def default_time_window_type():
        return "DEFAULT"

    def to_repr(self) -> dict:
        return {
            'type': self.moving_time_window_type(),
            'value': f"{self.value}{self.descriptor}" if self.value is not None else self.descriptor
        }

    @staticmethod
    def from_repr(raw_data: dict) -> MovingTimeWindow:
        time_window_type = raw_data['type'].upper()
        if time_window_type != MovingTimeWindow.moving_time_window_type():
            if time_window_type != MovingTimeWindow.default_time_window_type():
                raise ValueError(f"Unrecognized type [{time_window_type}] for MovingTimeWindow")

        return MovingTimeWindow(raw_data['value'])

    def __eq__(self, o) -> bool:
        if isinstance(o, MovingTimeWindow):
            return o.value == self.value and o.descriptor == self.descriptor
        else:
            return False


class FixedTimeWindow(TimeWindow):

    def __init__(self, start: datetime, end: datetime):
        self.start = start
        self.end = end

    @staticmethod
    def fixed_time_window_type():
        return "FIXED"

    @staticmethod
    @deprecation.deprecated(deprecated_in="1.5.0", removed_in="2.0.0", details="Use the fixed_time_window_type method instead")
    def custom_time_window_type():
        return "CUSTOM"

    def to_repr(self) -> dict:
        return {
            'type': self.fixed_time_window_type(),
            'start': self.start.isoformat(),
            'end': self.end.isoformat()
        }

    @staticmethod
    def from_isoformat(start: str, end: str) -> FixedTimeWindow:
        return FixedTimeWindow(datetime.fromisoformat(start), datetime.fromisoformat(end))

    @staticmethod
    def from_repr(raw_data: dict) -> FixedTimeWindow:
        time_window_type = raw_data['type'].upper()
        if time_window_type != FixedTimeWindow.fixed_time_window_type():
            if time_window_type != FixedTimeWindow.custom_time_window_type():
                raise ValueError(f"Unrecognized type [{time_window_type}] for FixedTimeWindow")

        return FixedTimeWindow.from_isoformat(raw_data['start'], raw_data['end'])

    def __eq__(self, o) -> bool:
        if isinstance(o, FixedTimeWindow):
            return o.start == self.start and o.end == self.end
        else:
            return False
