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


logger = logging.getLogger("logger.common.analytic.analytic")


class AnalyticComputation:

    @staticmethod
    def analytic_validity_check(analytic: dict):
        logger.info("ANALYTIC.DISPLACEMENT: " + str(analytic))

        # check if timespan is in the dict
        if 'timespan' not in analytic:
            logger.debug("timespan failed")
            return False

        # check if project is in the dict
        if 'project' not in analytic:
            logger.debug("project failed")
            return False

        # check if dimension is in the dict
        if 'dimension' not in analytic:
            logger.debug("dimension failed")
            return False

        # check if metric is in the dict
        if 'metric' not in analytic:
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

        # list the allowed dimensions and the allowed metrics split per sub_type
        allowed_dimensions = ["user", "message", "conversation", "dialogue", "bot"]
        if str(analytic['dimension']).lower() not in allowed_dimensions:
            logger.debug("dimension.value failed")
            return False

        allowed_metrics_user = ["u:total", "u:active", "u:engaged", "u:new"]
        allowed_metrics_message = ["m:from_users", "m:conversation", "m:from_bot", "m:responses", "m:notifications", "m:unhandled", "m:segmentation", "r:segmentation"]
        allowed_metrics_conversation = ["c:total", "c:new", "c:length", "c:path"]
        allowed_metrics_dialogue = ["d:fallback", "d:intents", "d:domains"]
        allowed_metrics_bot = ["b:response"]

        if str(analytic['dimension']).lower() == "user":
            if str(analytic['metric']).lower() not in allowed_metrics_user:
                logger.debug("metric.value failed")
                return False
        elif str(analytic['dimension']).lower() == "message":
            if str(analytic['metric']).lower() not in allowed_metrics_message:
                logger.debug("metric.value failed")
                return False
        elif str(analytic['dimension']).lower() == "conversation":
            if str(analytic['metric']).lower() not in allowed_metrics_conversation:
                logger.debug("metric.value failed")
                return False
        elif str(analytic['dimension']).lower() == "dialogue":
            if str(analytic['metric']).lower() not in allowed_metrics_dialogue:
                logger.debug("metric.value failed")
                return False
        elif str(analytic['dimension']).lower() == "bot":
            if str(analytic['metric']).lower() not in allowed_metrics_bot:
                logger.debug("metric.value failed")
                return False

        return True

    def compute_u_total(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": "userId.keyword"
                    }
                },
                "terms_count": {
                    "terms": {
                        "field": "userId.keyword",
                        "size": 65535
                    }
                }
            }
        }
        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        user_list = []

        for item in response['aggregations']['terms_count']['buckets']:
            user_list.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return response['aggregations']['type_count']['value'], user_list

    def compute_u_active(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type.keyword": "request"
                            }
                        }
                    ],
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": "userId.keyword"
                    }
                },
                "terms_count": {
                    "terms": {
                        "field": "userId.keyword",
                        "size": 65535
                    }
                }
            }
        }
        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        user_list = []

        for item in response['aggregations']['terms_count']['buckets']:
            user_list.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return response['aggregations']['type_count']['value'], user_list

    def compute_u_engaged(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type.keyword": "notification"
                            }
                        }
                    ],
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": "userId.keyword"
                    }
                },
                "terms_count": {
                    "terms": {
                        "field": "userId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        user_list = []

        for item in response['aggregations']['terms_count']['buckets']:
            user_list.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return response['aggregations']['type_count']['value'], user_list

    def compute_u_new(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "userId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        users_in_period = []
        for item in response['aggregations']['terms_count']['buckets']:
            users_in_period.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        users_in_period = set(users_in_period)

        body = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "lt": min_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "userId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        users_out_period = []
        for item in response['aggregations']['terms_count']['buckets']:
            users_out_period.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        users_out_period = set(users_out_period)

        final_users = users_in_period - users_out_period

        return len(final_users), list(final_users)

    def compute_m_from_users(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                    "type.keyword": "request"
                            }
                        }
                    ],
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "userId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        user_list = []
        total_counter = 0
        for item in response['aggregations']['terms_count']['buckets']:
            user_list.append(item['key'])
            total_counter = total_counter + int(item['doc_count'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return total_counter, user_list

    def compute_m_segmentation(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "type.keyword",
                        "size": 5
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        type_counter = {}
        for item in response['aggregations']['terms_count']['buckets']:
            type_counter[item['key']] = item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `5` but the number of types is higher")

        return type_counter

    def compute_r_segmentation(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type.keyword": "request"
                            }
                        }
                    ],
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "content.type.keyword",
                        "size": 10
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        type_counter = {}
        for item in response['aggregations']['terms_count']['buckets']:
            type_counter[item['key']] = item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `10` but the number of content types is higher")

        return type_counter

    def compute_m_conversation(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        conversation_list = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            conversation_list.append(item['key'])
            total_len = total_len + 1

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        return total_len, conversation_list

    def compute_m_from_bot(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type.keyword": "response"
                            }
                        }
                    ],
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            messages.append(item['key'])
            total_len = total_len + item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of response messages is higher")

        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type.keyword": "notification"
                            }
                        }
                    ],
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        for item in response['aggregations']['terms_count']['buckets']:
            messages.append(item['key'])
            total_len = total_len + item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of notification messages is higher")

        return total_len, messages

    def compute_m_responses(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type.keyword": "response"
                            }
                        }
                    ],
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            messages.append(item['key'])
            total_len = total_len + item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return total_len, messages

    def compute_m_notifications(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type.keyword": "notification"
                            }
                        }
                    ],
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            messages.append(item['key'])
            total_len = total_len + item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of notification messages is higher")

        return total_len, messages

    def compute_m_unhandled(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "handled.keyword": True
                            }
                        }
                    ],
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                messages.append(item['key'])
                total_len = total_len + item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of unhandled messages is higher")

        return total_len, messages

    def compute_c_total(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        conversation_list = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            conversation_list.append(item['key'])
            total_len = total_len + 1

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        return total_len, conversation_list

    def compute_c_new(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        conv_in_period = []
        for item in response['aggregations']['terms_count']['buckets']:
            conv_in_period.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        conv_in_period = set(conv_in_period)

        body = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "lt": min_bound
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        conv_out_period = []
        for item in response['aggregations']['terms_count']['buckets']:
            conv_out_period.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        conv_out_period = set(conv_out_period)

        final_conv = conv_in_period - conv_out_period

        return len(final_conv), list(final_conv)

    def compute_c_length(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        conversation_list = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            conversation_list.append({"conversation": item['key'], "length": item['doc_count']})
            total_len = total_len + 1

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        return total_len, conversation_list

    def compute_c_path(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        conversation_list = []
        for item in response['aggregations']['terms_count']['buckets']:
            conversation_list.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        conversation_list = list(set(conversation_list))
        paths = []

        for item in conversation_list:
            time_bound = self.support_bound_timestamp(analytic['timespan'])
            min_bound = time_bound[0]
            max_bound = time_bound[1]
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "conversationId.keyword": item
                                }
                            }
                        ],
                        "filter": [
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
                        ]
                    }
                },
                "aggs": {
                    "terms_count": {
                        "terms": {
                            "field": "messageId.keyword",
                            "size": 65535
                        }
                    }
                }
            }

            index = Utils.generate_index(data_type="message", project=project)
            response = es.search(index=index, body=body, size=0)
            message_list = []
            for obj in response['aggregations']['terms_count']['buckets']:
                message_list.append(obj['key'])

            paths.append({item: message_list})

            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of messages is higher")

        return len(paths), paths

    def compute_d_fallback(self, analytic: dict, es: Elasticsearch, project: str):

        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "intent.keyword": "default"
                            }
                        }
                    ],
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": "messageId.keyword"
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        total_missed = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            total_missed = response['aggregations']['type_count']['value']

        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": "messageId.keyword"
                    }
                }
            }
        }
        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        total_messages = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            total_messages = response['aggregations']['type_count']['value']

        return total_missed, total_messages

    def compute_d_intents(self, analytic: dict, es: Elasticsearch, project: str):

        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": "intent.keyword"
                    }
                },
                "terms_count": {
                    "terms": {
                        "field": "intent.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        intent_list = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                intent_list.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of intents is higher")

        value = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return value, intent_list

    def compute_d_domains(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": "domain.keyword"
                    }
                },
                "terms_count": {
                    "terms": {
                        "field": "domain.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        domain_list = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in \
                response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                domain_list.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of domains is higher")

        value = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in \
                response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return value, domain_list

    def compute_b_response(self, analytic: dict, es: Elasticsearch, project: str):
        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": "messageId.keyword"
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        total_messages = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in \
                response['aggregations']['type_count']:
            total_messages = response['aggregations']['type_count']['value']

        time_bound = self.support_bound_timestamp(analytic['timespan'])
        min_bound = time_bound[0]
        max_bound = time_bound[1]
        body = {
            "query": {
                "bool": {
                    "filter": [
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
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "userId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=project)
        response = es.search(index=index, body=body, size=0)
        total_not_working = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in \
                response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                if item['doc_count'] == 1:
                    total_not_working = total_not_working + 1

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return total_not_working, total_messages

    @staticmethod
    def support_bound_timestamp(time_object: dict):
        if str(time_object['type']).lower() == "default":
            if str(time_object['value']).upper() == "30D":
                now = datetime.datetime.now()
                delta = datetime.timedelta(days=30)
                temp_old = now - delta
                return temp_old.isoformat(), now.isoformat()
            elif str(time_object['value']).upper() == "10D":
                now = datetime.datetime.now()
                delta = datetime.timedelta(days=10)
                temp_old = now - delta
                return temp_old.isoformat(), now.isoformat()
            elif str(time_object['value']).upper() == "7D":
                now = datetime.datetime.now()
                delta = datetime.timedelta(days=7)
                temp_old = now - delta
                return temp_old.isoformat(), now.isoformat()
            elif str(time_object['value']).upper() == "1D":
                now = datetime.datetime.now()
                delta = datetime.timedelta(days=1)
                temp_old = now - delta
                return temp_old.isoformat(), now.isoformat()
            elif str(time_object['value']).upper() == "TODAY":
                now = datetime.datetime.now()
                temp_old = datetime.datetime(now.year, now.month, now.day)
                return temp_old.isoformat(), now.isoformat()
        else:
            start = time_object['start']
            end = time_object['end']
            return start, end
