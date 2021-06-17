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


class DefaultTime:

    DEFAULT_TIME_TYPE = "DEFAULT"
    ALLOWED_DEFAULT_TIME_VALUES = ["30D", "10D", "7D", "1D", "TODAY"]

    def __init__(self, value: str):
        self.value = value

    def to_repr(self) -> dict:
        return{
            'type': self.DEFAULT_TIME_TYPE,
            'value': self.value.upper()
        }

    @staticmethod
    def from_repr(raw_data: dict) -> DefaultTime:
        if 'type' in raw_data:
            if str(raw_data['type']).upper() != DefaultTime.DEFAULT_TIME_TYPE:
                raise ValueError("Unrecognized type for DefaultTime")
        else:
            raise ValueError("DefaultTime must contain a type")

        if 'value' not in raw_data:
            raise ValueError('Value must be defined in the DefaultTime object')

        if str(raw_data['value']).upper() not in DefaultTime.ALLOWED_DEFAULT_TIME_VALUES:
            raise ValueError('Unknown value for value in the DefaultTime')

        return DefaultTime(raw_data['value'])


class CustomTime:

    CUSTOM_TIME_TYPE = "CUSTOM"

    def __init__(self, start: datetime, end: datetime):
        self.start = start
        self.end = end

    def to_repr(self) -> dict:
        return{
            'type': self.CUSTOM_TIME_TYPE,
            'start': self.start.isoformat(),
            'end': self.end.isoformat()
        }

    @staticmethod
    def from_isoformat(start: str, end: str) -> CustomTime:
        return CustomTime(datetime.fromisoformat(start), datetime.fromisoformat(end))

    @staticmethod
    def from_repr(raw_data: dict) -> CustomTime:
        if 'type' in raw_data:
            if str(raw_data['type']).upper() != CustomTime.CUSTOM_TIME_TYPE:
                raise ValueError("Unrecognized type for CustomTime")
        else:
            raise ValueError("CustomTime must contain a type")

        if 'start' not in raw_data or 'end' not in raw_data:
            raise ValueError('Start and end must be defined in the CustomTime object')

        return CustomTime.from_isoformat(raw_data['start'], raw_data['end'])
