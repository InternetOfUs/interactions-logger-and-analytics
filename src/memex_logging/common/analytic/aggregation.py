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

from elasticsearch import Elasticsearch

from memex_logging.common.model.aggregation import Aggregation
from memex_logging.utils.utils import Utils


logger = logging.getLogger("logger.common.analytic.aggregation")


class AggregationComputation:

    @staticmethod
    def max_aggr(analytic: Aggregation, es: Elasticsearch):
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
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    @staticmethod
    def min_aggr(analytic: Aggregation, es: Elasticsearch):
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
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    @staticmethod
    def avg_aggr(analytic: Aggregation, es: Elasticsearch):
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
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    @staticmethod
    def cardinality_aggr(analytic: Aggregation, es: Elasticsearch):
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
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    @staticmethod
    def extended_stats_aggr(analytic: Aggregation, es: Elasticsearch):
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
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    @staticmethod
    def percentiles_aggr(analytic: Aggregation, es: Elasticsearch):
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
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    @staticmethod
    def stats_aggr(analytic: Aggregation, es: Elasticsearch):
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
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    @staticmethod
    def sum_aggr(analytic: Aggregation, es: Elasticsearch):
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
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    @staticmethod
    def value_count_aggr(analytic: Aggregation, es: Elasticsearch):
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
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']
