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

import logging

import dateutil
import datetime
from elasticsearch import Elasticsearch
from flask_restful import abort

class AggregationComputation:
    g_index = "message-memex*"

    def aggregation_validity_check(analytic: dict):
        logging.info("AGGREGATION.DISPLACEMENT: " + str(analytic))

        # check if timespan is in the dict
        if 'timespan' not in analytic:
            abort(500, message="AGGREGATION.MODEL.CHECK: timespan failed")
            return False

        # check if project is in the dict
        if 'project' not in analytic:
            abort(500, message="AGGREGATION.MODEL.CHECK: project failed")
            return False

        # check if dimension is in the dict
        if 'aggregation' not in analytic:
            abort(500, message="AGGREGATION.MODEL.CHECK: dimension failed")
            return False

        # check if metric is in the dict
        if 'field' not in analytic:
            abort(500, message="AGGREGATION.MODEL.CHECK: metric failed")
            return False

        # check timespan details
        if 'type' not in (analytic["timespan"]):
            abort(500, message="AGGREGATION.MODEL.SUBCHECK: timespan.type.key failed")
            return False

        if str(analytic['timespan']['type']).lower() not in ["default", "custom"]:
            abort(500, message="AGGREGATION.MODEL.SUBCHECK: timespan.type.value failed")
            return False

        allowed_time_defaults = ["30D", "10D", "7D", "1D", "TODAY"]
        if str(analytic['timespan']['type']).lower() == "default":
            if 'value' not in analytic['timespan']:
                abort(500, message="AGGREGATION.MODEL.SUBCHECK: timespan.value.key failed")
                return False
            else:
                if str(analytic['timespan']['value']).upper() not in allowed_time_defaults:
                    abort(500, message="AGGREGATION.MODEL.SUBCHECK: timespan.value.value failed")
                    return False

        if 'filters' in analytic:
            for item in analytic['filters']:
                if 'field' not in item:
                    abort(500, message="AGGREGATION.MODEL.SUBCHECK: filters.field failed")
                    return False
                if 'operation' not in item:
                    abort(500, message="AGGREGATION.MODEL.SUBCHECK: filters.operation failed")
                    return False
                allowed_operation = ["gre"]
                if item['operation'] not in allowed_operation:
                    abort(500, message="AGGREGATION.MODEL.SUBCHECK: filters.operation.value failed")
                    return False
                if 'value' not in item:
                    abort(500, message="AGGREGATION.MODEL.SUBCHECK: filters.value failed")
                    return False

        return True

    def max_aggr(self, analytic: dict, es: Elasticsearch, project:str):
        time_bound = self._support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        filters_dict = {"filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound
                                }
                            }
                        },
                        {
                            "range": {
                                "timestamp": {
                                    "lte": max_bound
                                }
                            }
                        }
                    ]}
        body = {
            "query": {
                "bool": filters_dict
            },
            "aggs": {
                "type_count": {
                    "max": {
                        "field": analytic['field']
                    }
                }
            }
        }
        response = es.search(index=self.g_index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def min_aggr(self, analytic: dict, es: Elasticsearch, project:str):
        time_bound = self._support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        filters_dict = {"filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound
                                }
                            }
                        },
                        {
                            "range": {
                                "timestamp": {
                                    "lte": max_bound
                                }
                            }
                        }
                    ]}
        body = {
            "query": {
                "bool": filters_dict
            },
            "aggs": {
                "type_count": {
                    "min": {
                        "field": analytic['field']
                    }
                }
            }
        }
        response = es.search(index=self.g_index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def avg_aggr(self, analytic: dict, es: Elasticsearch, project:str):
        time_bound = self._support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        filters_dict = {"filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound
                                }
                            }
                        },
                        {
                            "range": {
                                "timestamp": {
                                    "lte": max_bound
                                }
                            }
                        }
                    ]}
        body = {
            "query": {
                "bool": filters_dict
            },
            "aggs": {
                "type_count": {
                    "avg": {
                        "field": analytic['field']
                    }
                }
            }
        }
        response = es.search(index=self.g_index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def cardinality_aggr(self, analytic: dict, es: Elasticsearch, project:str):
        time_bound = self._support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        filters_dict = {"filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound
                                }
                            }
                        },
                        {
                            "range": {
                                "timestamp": {
                                    "lte": max_bound
                                }
                            }
                        }
                    ]}
        body = {
            "query": {
                "bool": filters_dict
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": analytic['field']
                    }
                }
            }
        }
        response = es.search(index=self.g_index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def extended_stats_aggr(self, analytic: dict, es: Elasticsearch, project:str):
        time_bound = self._support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        filters_dict = {"filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound
                                }
                            }
                        },
                        {
                            "range": {
                                "timestamp": {
                                    "lte": max_bound
                                }
                            }
                        }
                    ]}
        body = {
            "query": {
                "bool": filters_dict
            },
            "aggs": {
                "type_count": {
                    "extended_stats": {
                        "field": analytic['field']
                    }
                }
            }
        }
        response = es.search(index=self.g_index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def percentiles_aggr(self, analytic: dict, es: Elasticsearch, project:str):
        time_bound = self._support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        filters_dict = {"filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound
                                }
                            }
                        },
                        {
                            "range": {
                                "timestamp": {
                                    "lte": max_bound
                                }
                            }
                        }
                    ]}
        body = {
            "query": {
                "bool": filters_dict
            },
            "aggs": {
                "type_count": {
                    "percentiles": {
                        "field": analytic['field']
                    }
                }
            }
        }
        response = es.search(index=self.g_index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def stats_aggr(self, analytic: dict, es: Elasticsearch, project:str):
        time_bound = self._support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        filters_dict = {"filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound
                                }
                            }
                        },
                        {
                            "range": {
                                "timestamp": {
                                    "lte": max_bound
                                }
                            }
                        }
                    ]}
        body = {
            "query": {
                "bool": filters_dict
            },
            "aggs": {
                "type_count": {
                    "stats": {
                        "field": analytic['field']
                    }
                }
            }
        }
        response = es.search(index=self.g_index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def sum_aggr(self, analytic: dict, es: Elasticsearch, project:str):
        time_bound = self._support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        filters_dict = {"filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound
                                }
                            }
                        },
                        {
                            "range": {
                                "timestamp": {
                                    "lte": max_bound
                                }
                            }
                        }
                    ]}
        body = {
            "query": {
                "bool": filters_dict
            },
            "aggs": {
                "type_count": {
                    "sum": {
                        "field": analytic['field']
                    }
                }
            }
        }
        response = es.search(index=self.g_index, body=body, size=0)
        return response['aggregations']['type_count']['value']

    def value_count_aggr(self, analytic: dict, es: Elasticsearch, project:str):
        time_bound = self._support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        filters_dict = {"filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound
                                }
                            }
                        },
                        {
                            "range": {
                                "timestamp": {
                                    "lte": max_bound
                                }
                            }
                        }
                    ]}
        body = {
            "query": {
                "bool": filters_dict
            },
            "aggs": {
                "type_count": {
                    "sum": {
                        "field": analytic['field']
                    }
                }
            }
        }
        response = es.search(index=self.g_index, body=body, size=0)
        return response['aggregations']['type_count']['value']


    @staticmethod
    def _support_bound_timestamp(time_object: dict):
        if str(time_object['type']).lower() == "default":
            try:
                if str(time_object['value']).upper() == "30D":
                    now = datetime.datetime.now()
                    delta = datetime.timedelta(days=30)
                    temp_old = now - delta
                    min_bound = str(temp_old.year) + "-" + str(temp_old.month) + "-" + str(temp_old.day)
                    max_bound = str(now.year) + "-" + str(now.month) + "-" + str(now.day)
                    return min_bound, max_bound
                elif str(time_object['value']).upper() == "10D":
                    now = datetime.datetime.now()
                    delta = datetime.timedelta(days=10)
                    temp_old = now - delta
                    min_bound = str(temp_old.year) + "-" + str(temp_old.month) + "-" + str(temp_old.day)
                    max_bound = str(now.year) + "-" + str(now.month) + "-" + str(now.day)
                    return min_bound, max_bound
                elif str(time_object['value']).upper() == "7D":
                    now = datetime.datetime.now()
                    delta = datetime.timedelta(days=7)
                    temp_old = now - delta
                    min_bound = str(temp_old.year) + "-" + str(temp_old.month) + "-" + str(temp_old.day)
                    max_bound = str(now.year) + "-" + str(now.month) + "-" + str(now.day)
                    return min_bound, max_bound
                elif str(time_object['value']).upper() == "1D":
                    now = datetime.datetime.now()
                    delta = datetime.timedelta(days=1)
                    temp_old = now - delta
                    min_bound = str(temp_old.year) + "-" + str(temp_old.month) + "-" + str(temp_old.day)
                    max_bound = str(now.year) + "-" + str(now.month) + "-" + str(now.day)
                    return min_bound, max_bound
                elif str(time_object['value']).upper() == "TODAY":
                    now = datetime.datetime.now()
                    min_bound = str(now.year) + "-" + str(now.month) + "-" + str(now.day)
                    max_bound = str(now.year) + "-" + str(now.month) + "-" + str(now.day)
                    return min_bound, max_bound
            except:
                abort(500, message="ANALYTIC.COMPUTATION.TIMEBOUND: cannot generate a valid date")
        else:
            start = None
            end = None
            try:
                print(time_object['start'])
                start = datetime.datetime.strptime(time_object['start'], '%Y-%m-%d')  # %H:%M:%S.%f
            except:
                abort(500,
                      message="ANALYTIC.COMPUTATION.TIMEBOUND: cannot parse starting date. User a YYYY-MM-DD format")
            try:
                end = datetime.datetime.strptime(time_object['end'], '%Y-%m-%d')
            except:
                abort(500, message="ANALYTIC.COMPUTATION.TIMEBOUND: cannot parse ending date. User a YYYY-MM-DD format")
            return start, end