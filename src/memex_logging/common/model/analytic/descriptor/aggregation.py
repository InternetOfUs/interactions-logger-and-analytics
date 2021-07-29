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

from typing import List, Optional

from memex_logging.common.model.analytic.descriptor.common import CommonAnalyticDescriptor
from memex_logging.common.model.analytic.time import TimeWindow


class AggregationDescriptor(CommonAnalyticDescriptor):

    TYPE = "aggregation"
    ALLOWED_AGGREGATION_VALUES = ["avg", "min", "max", "sum", "stats", "extended_stats", "value_count", "cardinality", "percentiles"]

    def __init__(self, time_span: TimeWindow, project: str, field: str, aggregation: str, filters: Optional[List[Filter]] = None) -> None:
        super().__init__(time_span, project)
        self.field = field
        self.aggregation = aggregation.lower()
        self.filters = filters

        if self.filters is None:
            self.filters = []

    def to_repr(self) -> dict:
        return {
            'timespan': self.time_span.to_repr(),
            'project': self.project,
            'type': self.TYPE,
            'field': self.field,
            'aggregation': self.aggregation,
            'filters': [aggregation_filter.to_repr() for aggregation_filter in self.filters] if self.filters is not None else []
        }

    @staticmethod
    def from_repr(raw_data: dict) -> AggregationDescriptor:
        analytic_type = raw_data['type'].lower()
        if analytic_type != AggregationDescriptor.TYPE:
            raise ValueError(f"Unrecognized type [{analytic_type}] for AggregationDescriptor")

        timespan = TimeWindow.from_repr(raw_data['timespan'])

        aggregation = raw_data['aggregation'].lower()
        if aggregation not in AggregationDescriptor.ALLOWED_AGGREGATION_VALUES:
            raise ValueError(f"Unrecognized type [{aggregation}] of AggregationDescriptor")

        filters = None
        if raw_data.get('filters'):
            if isinstance(raw_data['filters'], list):
                filters = [Filter.from_repr(aggregation_filter) for aggregation_filter in raw_data['filters']]
            else:
                raise ValueError("Filters is not a list")

        return AggregationDescriptor(timespan, raw_data['project'], raw_data['field'], aggregation, filters)

    def __eq__(self, o) -> bool:
        if isinstance(o, AggregationDescriptor):
            return o.time_span == self.time_span and o.project == self.project and o.field == self.field and o.aggregation == self.aggregation and o.filters == self.filters
        else:
            return False


class Filter:

    ALLOWED_OPERATIONS = ["gr", "gre", "lq", "lqe", "term", "match"]

    def __init__(self, field: str, operation: str, value: str):
        self.field = field
        self.operation = operation.lower()
        self.value = value

    def to_repr(self) -> dict:
        return {
            'field': self.field,
            'operation': self.operation,
            'value': self.value
        }

    @staticmethod
    def from_repr(raw_data: dict) -> Filter:
        operation = raw_data['operation'].lower()
        if operation not in Filter.ALLOWED_OPERATIONS:
            raise ValueError(f"Unknown value [{operation}] for operation")

        return Filter(raw_data['field'], operation, raw_data['value'])

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
        missing = None
        if 'missing' in data:
            missing = data['missing']

        return Average(data['field'], missing)


class WeightedAverage:

    ALLOWED_DATA_FORMATS = ["numeric", "percentage"]

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
        data_format = "numeric"
        if 'format' in data:
            if data['format'].lower() not in WeightedAverage.ALLOWED_DATA_FORMATS:
                raise ValueError(f"unknown type [{data['format']}] for format")
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
        percents = []
        if 'percents' in data:
            if isinstance(data['percents'], list):
                percents += data['percents']
            else:
                raise ValueError("percents is not a list")
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
        compression = None
        if 'compression' in data:
            compression = data['compression']

        return MedianAbsoluteDeviation(data['field'], compression)
