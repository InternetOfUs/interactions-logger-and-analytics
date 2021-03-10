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

import logging

import datetime
from elasticsearch import Elasticsearch

from memex_logging.utils.utils import Utils


logger = logging.getLogger("logger.common.analytic.aggregation")


class AggregationComputation:

    @staticmethod
    def aggregation_validity_check(analytic: dict):
        logging.info("AGGREGATION.DISPLACEMENT: " + str(analytic))

        # check if timespan is in the dict
        if 'timespan' not in analytic:
            logger.debug("timespan failed")
            return False

        # check if project is in the dict
        if 'project' not in analytic:
            logger.debug("project failed")
            return False

        # check if aggregation is in the dict
        if 'aggregation' not in analytic:
            logger.debug("dimension failed")
            return False

        # check if field is in the dict
        if 'field' not in analytic:
            logger.debug("metric failed")
            return False

        # check timespan details
        if 'type' not in (analytic["timespan"]):
            logger.debug("timespan.type.key failed")
            return False

        if str(analytic['timespan']['type']).lower() not in ["default", "custom"]:
            logger.debug("timespan.type.value failed")
            return False

        allowed_time_defaults = ["30D", "10D", "7D", "1D", "TODAY"]
        if str(analytic['timespan']['type']).lower() == "default":
            if 'value' not in analytic['timespan']:
                logger.debug("timespan.value.key failed")
                return False
            else:
                if str(analytic['timespan']['value']).upper() not in allowed_time_defaults:
                    logger.debug("timespan.value.value failed")
                    return False
        else:
            try:
                datetime.datetime.fromisoformat(analytic['timespan']['start']).isoformat()
                datetime.datetime.fromisoformat(analytic['timespan']['end']).isoformat()
            except Exception as e:
                logger.debug("timespan.start or timespan.end failed", exc_info=e)
                return False

        if 'filters' in analytic:
            for item in analytic['filters']:
                if 'field' not in item:
                    logger.debug("filters.field failed")
                    return False
                if 'operation' not in item:
                    logger.debug("filters.operation failed")
                    return False
                allowed_operation = ["gre"]
                if item['operation'] not in allowed_operation:
                    logger.debug("filters.operation.value failed")
                    return False
                if 'value' not in item:
                    logger.debug("filters.value failed")
                    return False

        return True

    def max_aggr(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound,
                                    "lte": max_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "max": {
                        "field": analytic['field']
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def min_aggr(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound,
                                    "lte": max_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "min": {
                        "field": analytic['field']
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def avg_aggr(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound,
                                    "lte": max_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "avg": {
                        "field": analytic['field']
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def cardinality_aggr(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound,
                                    "lte": max_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": analytic['field']
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def extended_stats_aggr(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound,
                                    "lte": max_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "extended_stats": {
                        "field": analytic['field']
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def percentiles_aggr(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound,
                                    "lte": max_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "percentiles": {
                        "field": analytic['field']
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def stats_aggr(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound,
                                    "lte": max_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "stats": {
                        "field": analytic['field']
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def sum_aggr(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound,
                                    "lte": max_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "sum": {
                        "field": analytic['field']
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def value_count_aggr(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound,
                                    "lte": max_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "sum": {
                        "field": analytic['field']
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        return response['aggregations']['type_count']['value']
