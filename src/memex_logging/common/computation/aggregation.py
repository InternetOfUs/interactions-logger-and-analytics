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

import logging
from datetime import datetime
from typing import Optional

from elasticsearch import Elasticsearch

from memex_logging.common.model.analytic.descriptor.aggregation import AggregationDescriptor
from memex_logging.common.model.analytic.result.aggregation import AggregationResult
from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.common.analytic.aggregation")


class AggregationComputation:

    def __init__(self, es: Elasticsearch) -> None:
        self.es = es

    def get_result(self, analytic: AggregationDescriptor) -> Optional[AggregationResult]:
        if analytic.aggregation.lower() == "max":
            result = self._max(analytic)
        elif analytic.aggregation.lower() == "min":
            result = self._min(analytic)
        elif analytic.aggregation.lower() == "avg":
            result = self._avg(analytic)
        elif analytic.aggregation.lower() == "stats":
            result = self._stats(analytic)
        elif analytic.aggregation.lower() == "sum":
            result = self._sum(analytic)
        elif analytic.aggregation.lower() == "value_count":
            result = self._value_count(analytic)
        elif analytic.aggregation.lower() == "cardinality":
            result = self._cardinality(analytic)
        elif analytic.aggregation.lower() == "extended_stats":
            result = self._extended_stats(analytic)
        elif analytic.aggregation.lower() == "percentiles":
            result = self._percentiles(analytic)
        else:
            logger.info(f"Unrecognized type [{analytic.aggregation}] of aggregation")
            raise ValueError(f"Unrecognized type [{analytic.aggregation}] of aggregation")

        return result

    def _max(self, analytic: AggregationDescriptor) -> Optional[AggregationResult]:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat() if min_bound is not None else None,
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "max": {
                        "field": analytic.field
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message")
        response = self.es.search(index=index, body=body, size=0)
        value = None
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return AggregationResult({"max": value}, datetime.now(), min_bound, max_bound) if value is not None else None

    def _min(self, analytic: AggregationDescriptor) -> Optional[AggregationResult]:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat() if min_bound is not None else None,
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "min": {
                        "field": analytic.field
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message")
        response = self.es.search(index=index, body=body, size=0)
        value = None
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return AggregationResult({"min": value}, datetime.now(), min_bound, max_bound) if value is not None else None

    def _avg(self, analytic: AggregationDescriptor) -> Optional[AggregationResult]:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat() if min_bound is not None else None,
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "avg": {
                        "field": analytic.field
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message")
        response = self.es.search(index=index, body=body, size=0)
        value = None
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return AggregationResult({"avg": value}, datetime.now(), min_bound, max_bound) if value is not None else None

    def _cardinality(self, analytic: AggregationDescriptor) -> Optional[AggregationResult]:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat() if min_bound is not None else None,
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": analytic.field
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message")
        response = self.es.search(index=index, body=body, size=0)
        value = None
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return AggregationResult({"cardinality": value}, datetime.now(), min_bound, max_bound) if value is not None else None

    def _extended_stats(self, analytic: AggregationDescriptor) -> Optional[AggregationResult]:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat() if min_bound is not None else None,
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "extended_stats": {
                        "field": analytic.field
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message")
        response = self.es.search(index=index, body=body, size=0)
        value = None
        if 'aggregations' in response and 'type_count' in response['aggregations']:
            value = response['aggregations']['type_count']

        return AggregationResult(value, datetime.now(), min_bound, max_bound) if value is not None else None

    def _percentiles(self, analytic: AggregationDescriptor) -> Optional[AggregationResult]:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat() if min_bound is not None else None,
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "percentiles": {
                        "field": analytic.field
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message")
        response = self.es.search(index=index, body=body, size=0)
        value = None
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'values' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['values']

        return AggregationResult(value, datetime.now(), min_bound, max_bound) if value is not None else None

    def _stats(self, analytic: AggregationDescriptor) -> Optional[AggregationResult]:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat() if min_bound is not None else None,
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "stats": {
                        "field": analytic.field
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message")
        response = self.es.search(index=index, body=body, size=0)
        value = None
        if 'aggregations' in response and 'type_count' in response['aggregations']:
            value = response['aggregations']['type_count']

        return AggregationResult(value, datetime.now(), min_bound, max_bound) if value is not None else None

    def _sum(self, analytic: AggregationDescriptor) -> Optional[AggregationResult]:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat() if min_bound is not None else None,
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "sum": {
                        "field": analytic.field
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message")
        response = self.es.search(index=index, body=body, size=0)
        value = None
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return AggregationResult({"sum": value}, datetime.now(), min_bound, max_bound) if value is not None else None

    def _value_count(self, analytic: AggregationDescriptor) -> Optional[AggregationResult]:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat() if min_bound is not None else None,
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "sum": {
                        "field": analytic.field
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message")
        response = self.es.search(index=index, body=body, size=0)
        value = None
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return AggregationResult({"value_count": value}, datetime.now(), min_bound, max_bound) if value is not None else None
