from __future__ import annotations, absolute_import

import logging


class Analytic:
    def __init__(self, timespan) -> None:
        self.timespan = timespan

    def to_repr(self) -> dict:
        return {
            'timespan': self.timespan.to_repr()
        }

    @staticmethod
    def from_rep(data: dict) -> Analytic:
        timespan = None
        if 'timespan' in data:
            if str(data['timespan']['type']).lower() == "default":
                timespan = DefaultTime.from_rep(data['timespan'])
            elif str(data['timespan']['type']).lower() == "custom":
                timespan = CustomTime.from_rep(data['timespan'])
            else:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("An Analytic must contain a timespan")

        return Analytic(timespan)


class DefaultTime:
    def __init__(self, value: str):
        self.value = value

    def to_repr(self) -> dict:
        return{
            'type': 'DEFAULT',
            'value': self.value.upper()
        }

    @staticmethod
    def from_rep(data: dict) -> DefaultTime:
        if 'value' not in data:
            raise ValueError('a Value must be defined in the timespan object')

        if str(data['value']).lower() not in ['today', 'yesterday', '7d', '30d']:
            raise ValueError('unknown value for Value in the timespan')

        return DefaultTime(data['value'])


class CustomTime:
    def __init__(self, value: str):
        self.value = value

    def to_repr(self) -> dict:
        return{
            'type': 'default',
            'value': self.value
        }

    @staticmethod
    def from_rep(data: dict) -> CustomTime:
        if 'value' not in data:
            raise ValueError('a Value must be defined in the timespan object')

        if str(data['value']).lower() not in ['today', 'yesterday', '7d', '30d']:
            raise ValueError('unknown value for Value in the timespan')

        return CustomTime(data['value'])


