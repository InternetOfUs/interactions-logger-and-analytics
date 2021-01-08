# Copyright 2020 U-Hopper srl
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


class Analytic:
    def __init__(self, timespan, metric) -> None:
        self.timespan = timespan
        self.metric = metric

    def to_repr(self) -> dict:
        return {
            'timespan': self.timespan.to_repr(),
            'metric': self.metric.to_repr()
        }

    @staticmethod
    def from_rep(data: dict) -> Analytic:
        if 'timespan' in data:
            if str(data['timespan']['type']).lower() == "default":
                timespan = DefaultTime.from_rep(data['timespan'])
            elif str(data['timespan']['type']).lower() == "custom":
                timespan = CustomTime.from_rep(data['timespan'])
            else:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("An Analytic must contain a timespan")

        if 'metric' in data:
            if str(data['metric']['type']).lower() == "avg":
                metric = Average.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "wavg":
                metric = WeightedAverage.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "cardinality":
                metric = Cardinality.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "geobounds":
                metric = GeoBounds.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "geocentrality":
                metric = GeoCentrality.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "max":
                metric = Max.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "min":
                metric = Min.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "percentiles":
                metric = Percentiles.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "sum":
                metric = Sum.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "stats":
                metric = Stats.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "estats":
                metric = ExtendedStats.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "vcount":
                metric = ValueCount.from_rep(data['metric'])
            elif str(data['metric']['type']).lower() == "mad":
                metric = MedianAbsoluteDeviation.from_rep(data['metric'])
            else:
                raise ValueError("Unrecognized type for metric")
        else:
            raise ValueError("An Analytic must contain a metric")

        return Analytic(timespan, metric)


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


class Average:
    def __init__(self, field: str, missing):
        self.field = field
        self.missing = missing

    def to_repr(self) -> dict:
        return {
            'type': ' AVG',
            'field': self.field,
            'missing': self.missing
        }

    @staticmethod
    def from_rep(data: dict) -> Average:
        if 'field' not in data:
            raise ValueError('a field must be defined in an Average object')
        missing = None
        if 'missing' in data:
            missing = data['missing']
        return Average(data['field'], missing)


class WeightedAverage:
    def __init__(self, field: str, weight: str, missing=None, data_format=None):
        self.field = field
        self.weight = weight
        self.missing = missing
        self.data_format = data_format

    def to_repr(self) -> dict:
        return {
            'type': 'WAVG',
            'field': self.field,
            'missing': self.missing,
            'weight': self.weight,
            'format': self.data_format
        }

    @staticmethod
    def from_rep(data: dict) -> WeightedAverage:
        if 'field' not in data:
            raise ValueError('a field must be defined in an WeightedAverage object')
        if 'weight' not in data:
            raise ValueError('a weight must be defined in an WeightedAverage object')
        data_format = "NUMERIC"
        if 'format' in data:
            if str(data['format']).lower() not in ['numeric', 'percentage']:
                raise ValueError('unknown type for format in a WeightedAverage object')
            else:
                data_format = data['format']
        missing = None
        if 'missing' in data:
            missing = data['missing']
        return WeightedAverage(data['field'], data['weight'], missing=missing, data_format=data_format)


class Cardinality:
    def __init__(self, field: str, precision_threshold: int):
        self.field = field
        self.precision_threshold = precision_threshold

    def to_repr(self) -> dict:
        return {
            'type': ' CARDINALITY',
            'field': self.field,
            'precisionThreshold': self.precision_threshold
        }

    @staticmethod
    def from_rep(data: dict) -> Cardinality:
        if 'field' not in data:
            raise ValueError('a field must be defined in an Cardinality object')
        precision_threshold = None
        if 'precisionThreshold' in data:
            precision_threshold = data['precisionThreshold']
        return Cardinality(data['field'], precision_threshold)


class GeoBounds:
    def __init__(self, field: str, wrap_longitude: bool):
        self.field = field
        self.wrap_longitude = wrap_longitude

    def to_repr(self) -> dict:
        return {
            'type': ' GEOBOUNDS',
            'field': self.field,
            'wrapLongitude': self.wrap_longitude
        }

    @staticmethod
    def from_rep(data: dict) -> GeoBounds:
        if 'field' not in data:
            raise ValueError('a field must be defined in an GeoBounds object')
        wrap_longitude = True
        if 'wrapLongitude' in data:
            wrap_longitude = data['wrapLongitude']
        return GeoBounds(data['field'], wrap_longitude)


class GeoCentrality:
    def __init__(self, field: str):
        self.field = field

    def to_repr(self) -> dict:
        return {
            'type': ' GEOCENTRALITY',
            'field': self.field
        }

    @staticmethod
    def from_rep(data: dict) -> GeoCentrality:
        if 'field' not in data:
            raise ValueError('a field must be defined in an GeoCentrality object')
        return GeoCentrality(data['field'])


class Max:
    def __init__(self, field: str, missing):
        self.field = field
        self.missing = missing

    def to_repr(self) -> dict:
        return {
            'type': ' MAX',
            'field': self.field,
            'missing': self.missing
        }

    @staticmethod
    def from_rep(data: dict) -> Max:
        if 'field' not in data:
            raise ValueError('a field must be defined in an Max object')
        missing = None
        if 'missing' in data:
            missing = data['missing']
        return Max(data['field'], missing)


class Min:
    def __init__(self, field: str, missing):
        self.field = field
        self.missing = missing

    def to_repr(self) -> dict:
        return {
            'type': ' MIN',
            'field': self.field,
            'missing': self.missing
        }

    @staticmethod
    def from_rep(data: dict) -> Min:
        if 'field' not in data:
            raise ValueError('a field must be defined in an Min object')
        missing = None
        if 'missing' in data:
            missing = data['missing']
        return Min(data['field'], missing)


class Percentiles:
    def __init__(self, field: str, percents: list):
        self.field = field
        self.percents = percents

    def to_repr(self) -> dict:
        return {
            'type': 'PERCENTILES',
            'field': self.field,
            'percents': self.percents
        }

    @staticmethod
    def from_rep(data: dict) -> Percentiles:
        if 'field' not in data:
            raise ValueError('a field must be defined in an Percentiles object')
        percents = []
        if 'percents' in data:
            if isinstance(data['percents'], list):
                percents += data['percents']
            else:
                raise ValueError("percents must be a list")
        else:
            percents += [1, 5, 25, 50, 75, 95, 99]
        return Percentiles(data['field'], percents)


class Sum:
    def __init__(self, field: str, missing):
        self.field = field
        self.missing = missing

    def to_repr(self) -> dict:
        return {
            'type': ' SUM',
            'field': self.field,
            'missing': self.missing
        }

    @staticmethod
    def from_rep(data: dict) -> Sum:
        if 'field' not in data:
            raise ValueError('a field must be defined in an Sum object')
        missing = None
        if 'missing' in data:
            missing = data['missing']
        return Sum(data['field'], missing)


class Stats:
    def __init__(self, field: str, missing):
        self.field = field
        self.missing = missing

    def to_repr(self) -> dict:
        return {
            'type': ' STATS',
            'field': self.field,
            'missing': self.missing
        }

    @staticmethod
    def from_rep(data: dict) -> Stats:
        if 'field' not in data:
            raise ValueError('a field must be defined in an Stats object')
        missing = None
        if 'missing' in data:
            missing = data['missing']
        return Stats(data['field'], missing)


class ExtendedStats:
    def __init__(self, field: str, missing):
        self.field = field
        self.missing = missing

    def to_repr(self) -> dict:
        return {
            'type': ' ESTATS',
            'field': self.field,
            'missing': self.missing
        }

    @staticmethod
    def from_rep(data: dict) -> ExtendedStats:
        if 'field' not in data:
            raise ValueError('a field must be defined in an ExtendedStats object')
        missing = None
        if 'missing' in data:
            missing = data['missing']
        return ExtendedStats(data['field'], missing)


class ValueCount:
    def __init__(self, field: str):
        self.field = field

    def to_repr(self) -> dict:
        return {
            'type': ' VALUECOUNT',
            'field': self.field
        }

    @staticmethod
    def from_rep(data: dict) -> ValueCount:
        if 'field' not in data:
            raise ValueError('a field must be defined in an ValueCount object')
        return ValueCount(data['field'])


class MedianAbsoluteDeviation:
    def __init__(self, field: str, compression):
        self.field = field
        self.compression = compression

    def to_repr(self) -> dict:
        return {
            'type': ' ESTATS',
            'field': self.field,
            'compression': self.compression
        }

    @staticmethod
    def from_rep(data: dict) -> MedianAbsoluteDeviation:
        if 'field' not in data:
            raise ValueError('a field must be defined in an MedianAbsoluteDeviation object')
        compression = None
        if 'compression' in data:
            compression = data['compression']
        return MedianAbsoluteDeviation(data['field'], compression)
