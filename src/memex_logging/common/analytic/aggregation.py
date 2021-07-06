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
from typing import Union

from elasticsearch import Elasticsearch

from memex_logging.common.model.aggregation import AggregationAnalytic
from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.common.analytic.aggregation")


class AggregationComputation:

    def __init__(self, es: Elasticsearch) -> None:
        self.es = es

    def get_aggregation_result(self, analytic: AggregationAnalytic) -> Union[int, float, dict]:
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

    def _max(self, analytic: AggregationAnalytic) -> float:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                                    "gte": min_bound.isoformat(),
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def _min(self, analytic: AggregationAnalytic) -> float:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                                    "gte": min_bound.isoformat(),
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def _avg(self, analytic: AggregationAnalytic) -> float:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                                    "gte": min_bound.isoformat(),
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def _cardinality(self, analytic: AggregationAnalytic) -> int:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                                    "gte": min_bound.isoformat(),
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def _extended_stats(self, analytic: AggregationAnalytic) -> dict:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                                    "gte": min_bound.isoformat(),
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']

    def _percentiles(self, analytic: AggregationAnalytic) -> dict:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                                    "gte": min_bound.isoformat(),
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['values']

    def _stats(self, analytic: AggregationAnalytic) -> dict:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                                    "gte": min_bound.isoformat(),
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']

    def _sum(self, analytic: AggregationAnalytic) -> float:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                                    "gte": min_bound.isoformat(),
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def _value_count(self, analytic: AggregationAnalytic) -> float:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                                    "gte": min_bound.isoformat(),
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']
