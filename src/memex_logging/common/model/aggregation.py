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

from typing import Union, List, Optional

from memex_logging.common.model.time import DefaultTime, CustomTime


class Aggregation:

    AGGREGATION_TYPE = "aggregation"
    ALLOWED_AGGREGATION_VALUES = ["avg", "min", "max", "sum", "stats", "extended_stats", "value_count", "cardinality", "percentiles"]

    def __init__(self, timespan: Union[DefaultTime, CustomTime], project: str, field: str, aggregation: str, filters: Optional[List[Filter]] = None) -> None:
        self.timespan = timespan
        self.project = project
        self.field = field
        self.aggregation = aggregation
        self.filters = filters

        if self.filters is None:
            self.filters = []

    def to_repr(self) -> dict:
        return {
            'timespan': self.timespan.to_repr(),
            'project': self.project,
            'type': self.AGGREGATION_TYPE,
            'field': self.field,
            'aggregation': self.aggregation,
            'filters': [aggregation_filter.to_repr() for aggregation_filter in self.filters] if self.filters is not None else []
        }

    @staticmethod
    def from_repr(raw_data: dict) -> Aggregation:
        if 'type' in raw_data:
            if str(raw_data['type']).lower() != Aggregation.AGGREGATION_TYPE:
                raise ValueError("Unrecognized type for Aggregation")
        else:
            raise ValueError("Aggregation must contain a type")

        if 'project' not in raw_data:
            raise ValueError("Aggregation must contain a project")

        if 'field' not in raw_data:
            raise ValueError("Aggregation must contain a field")

        if 'timespan' in raw_data:
            if str(raw_data['timespan']['type']).upper() == DefaultTime.DEFAULT_TIME_TYPE:
                timespan = DefaultTime.from_repr(raw_data['timespan'])
            elif str(raw_data['timespan']['type']).upper() == CustomTime.CUSTOM_TIME_TYPE:
                timespan = CustomTime.from_repr(raw_data['timespan'])
            else:
                raise ValueError("Unrecognized type for timespan")
        else:
            raise ValueError("Aggregation must contain a timespan")

        if 'aggregation' in raw_data:
            if str(raw_data['aggregation']).lower() not in Aggregation.ALLOWED_AGGREGATION_VALUES:
                raise ValueError("Unrecognized type of aggregation")
        else:
            raise ValueError("An aggregation must be present in an Aggregation object")

        if raw_data.get('filters'):
            if isinstance(raw_data['filters'], list):
                filters = [Filter.from_repr(aggregation_filter) for aggregation_filter in raw_data['filters']]
            else:
                raise ValueError("Filters must be a list")
        else:
            filters = None

        return Aggregation(timespan, raw_data['project'], raw_data['field'], raw_data['aggregation'], filters)

    def __eq__(self, o) -> bool:
        if isinstance(o, Aggregation):
            return o.timespan == self.timespan and o.project == self.project and o.field == self.field and o.aggregation == self.aggregation and o.filters == self.filters
        else:
            return False


class Filter:

    ALLOWED_OPERATIONS = ["gr", "gre", "lq", "lqe", "term", "match"]

    def __init__(self, field: str, operation: str, value: str):
        self.field = field
        self.operation = operation
        self.value = value

    def to_repr(self) -> dict:
        return {
            'field': self.field,
            'operation': self.operation,
            'value': self.value
        }

    @staticmethod
    def from_repr(raw_data: dict) -> Filter:
        if 'field' not in raw_data:
            raise ValueError('Field must be defined in an Filter object')

        if 'operation' in raw_data:
            if str(raw_data['operation']).lower() not in Filter.ALLOWED_OPERATIONS:
                raise ValueError('Unknown value for operation in the UserAnalytic')
        else:
            raise ValueError('Operation must be defined in an Filter object')

        if 'value' not in raw_data:
            raise ValueError('Value must be defined in an Filter object')

        return Filter(raw_data['field'], raw_data['operation'], raw_data['value'])

    def __eq__(self, o) -> bool:
        if isinstance(o, Filter):
            return o.field == self.field and o.operation == self.operation and o.value == self.value
        else:
            return False


# These classes was defined previously but never used nor documented
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
    def from_repr(data: dict) -> Average:
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
    def from_repr(data: dict) -> WeightedAverage:
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
    def from_repr(data: dict) -> Cardinality:
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
    def from_repr(data: dict) -> GeoBounds:
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
    def from_repr(data: dict) -> GeoCentrality:
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
    def from_repr(data: dict) -> Max:
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
    def from_repr(data: dict) -> Min:
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
    def from_repr(data: dict) -> Percentiles:
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
    def from_repr(data: dict) -> Sum:
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
    def from_repr(data: dict) -> Stats:
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
    def from_repr(data: dict) -> ExtendedStats:
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
    def from_repr(data: dict) -> ValueCount:
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
    def from_repr(data: dict) -> MedianAbsoluteDeviation:
        if 'field' not in data:
            raise ValueError('a field must be defined in an MedianAbsoluteDeviation object')
        compression = None
        if 'compression' in data:
            compression = data['compression']
        return MedianAbsoluteDeviation(data['field'], compression)
