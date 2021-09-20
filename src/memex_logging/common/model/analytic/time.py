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

    @staticmethod
    def allowed_types():
        return [
            MovingTimeWindow.type(),
            FixedTimeWindow.type(),
            # MovingTimeWindow.deprecated_type(),
            # FixedTimeWindow.deprecated_type()
        ]

    @abstractmethod
    def to_repr(self) -> dict:
        pass

    @staticmethod
    @abstractmethod
    def from_repr(raw_data: dict) -> TimeWindow:
        time_window_type = raw_data['type'].lower()
        if time_window_type == MovingTimeWindow.type() or time_window_type == MovingTimeWindow.deprecated_type():
            time_span = MovingTimeWindow.from_repr(raw_data)
        elif time_window_type == FixedTimeWindow.type() or time_window_type == FixedTimeWindow.deprecated_type():
            time_span = FixedTimeWindow.from_repr(raw_data)
        else:
            raise ValueError(f"Unrecognized type [{time_window_type}] for TimeWindow")

        return time_span


class MovingTimeWindow(TimeWindow):

    def __init__(self, value: str):
        match = re.match(r"^([0-9]+)([dDwWmMyY])$", value)
        if match is not None:
            self.value = int(match.group(1))
            self.descriptor = match.group(2)
        else:
            if value.lower() == "today":
                self.value = None
                self.descriptor = "today"
            elif value.lower() == "all":
                self.value = None
                self.descriptor = "all"
            else:
                raise ValueError("Incorrect format for MovingTimeWindow value")

    @staticmethod
    def type():
        return "moving"

    @staticmethod
    @deprecation.deprecated(deprecated_in="1.5.0", removed_in="2.0.0", details="Use the moving_time_window_type method instead")
    def deprecated_type():
        return "default"

    def to_repr(self) -> dict:
        return {
            'type': self.type(),
            'value': f"{self.value}{self.descriptor}" if self.value is not None else self.descriptor
        }

    @staticmethod
    def from_repr(raw_data: dict) -> MovingTimeWindow:
        window_type = raw_data['type'].lower()
        if window_type != MovingTimeWindow.type() and window_type != MovingTimeWindow.deprecated_type():
            raise ValueError("Unrecognized type for MovingTimeWindow")

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
    def type():
        return "fixed"

    @staticmethod
    @deprecation.deprecated(deprecated_in="1.5.0", removed_in="2.0.0", details="Use the fixed_time_window_type method instead")
    def deprecated_type():
        return "custom"

    def to_repr(self) -> dict:
        return {
            'type': self.type(),
            'start': self.start.isoformat(),
            'end': self.end.isoformat()
        }

    @staticmethod
    def from_isoformat(start: str, end: str) -> FixedTimeWindow:
        return FixedTimeWindow(datetime.fromisoformat(start), datetime.fromisoformat(end))

    @staticmethod
    def from_repr(raw_data: dict) -> FixedTimeWindow:
        window_type = raw_data['type'].lower()
        if window_type != FixedTimeWindow.type() and window_type != FixedTimeWindow.deprecated_type():
            raise ValueError("Unrecognized type for FixedTimeWindow")

        return FixedTimeWindow.from_isoformat(raw_data['start'], raw_data['end'])

    def __eq__(self, o) -> bool:
        if isinstance(o, FixedTimeWindow):
            return o.start == self.start and o.end == self.end
        else:
            return False
