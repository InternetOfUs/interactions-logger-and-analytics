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

import datetime
from elasticsearch import Elasticsearch

from memex_logging.common.model.analytic import UserAnalytic, MessageAnalytic, TaskAnalytic, TransactionAnalytic, \
    ConversationAnalytic, DialogueAnalytic, BotAnalytic
from memex_logging.common.model.time import DefaultTime, CustomTime
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

        if str(analytic['timespan']['type']).lower() not in [DefaultTime.DEFAULT_TIME_TYPE, CustomTime.CUSTOM_TIME_TYPE]:
            logger.debug("timespan.type.value failed")
            return False

        if str(analytic['timespan']['type']).upper() == DefaultTime.DEFAULT_TIME_TYPE:
            if 'value' not in analytic['timespan']:
                logger.debug("timespan.value.key failed")
                return False
            else:
                if str(analytic['timespan']['value']).upper() not in DefaultTime.ALLOWED_DEFAULT_TIME_VALUES:
                    logger.debug("timespan.value.value failed")
                    return False
        else:
            try:
                datetime.datetime.fromisoformat(analytic['timespan']['start']).isoformat()
                datetime.datetime.fromisoformat(analytic['timespan']['end']).isoformat()
            except Exception as e:
                logger.debug("timespan.start or timespan.end failed", exc_info=e)
                return False

        if str(analytic['dimension']).lower() == UserAnalytic.USER_DIMENSION:
            if str(analytic['metric']).lower() not in UserAnalytic.ALLOWED_USER_METRIC_VALUES:
                logger.debug("metric.value failed")
                return False
        elif str(analytic['dimension']).lower() == MessageAnalytic.MESSAGE_DIMENSION:
            if str(analytic['metric']).lower() not in MessageAnalytic.ALLOWED_MESSAGE_METRIC_VALUES:
                logger.debug("metric.value failed")
                return False
        elif str(analytic['dimension']).lower() == TaskAnalytic.TASK_DIMENSION:
            if str(analytic['metric']).lower() not in TaskAnalytic.ALLOWED_TASK_METRIC_VALUES:
                logger.debug("metric.value failed")
                return False
        elif str(analytic['dimension']).lower() == TransactionAnalytic.TRANSACTION_DIMENSION:
            if str(analytic['metric']).lower() not in TransactionAnalytic.ALLOWED_TRANSACTION_METRIC_VALUES:
                logger.debug("metric.value failed")
                return False
        elif str(analytic['dimension']).lower() == ConversationAnalytic.CONVERSATION_DIMENSION:
            if str(analytic['metric']).lower() not in ConversationAnalytic.ALLOWED_CONVERSATION_METRIC_VALUES:
                logger.debug("metric.value failed")
                return False
        elif str(analytic['dimension']).lower() == DialogueAnalytic.DIALOGUE_DIMENSION:
            if str(analytic['metric']).lower() not in DialogueAnalytic.ALLOWED_DIALOGUE_METRIC_VALUES:
                logger.debug("metric.value failed")
                return False
        elif str(analytic['dimension']).lower() == BotAnalytic.BOT_DIMENSION:
            if str(analytic['metric']).lower() not in BotAnalytic.ALLOWED_BOT_METRIC_VALUES:
                logger.debug("metric.value failed")
                return False
        else:
            logger.debug("dimension.value failed")
            return False

        return True

    def compute_u_total(self, analytic: dict, es: Elasticsearch, project: str):
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
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
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
                        },
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
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
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
                        },
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
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
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
                        },
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
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
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
                        },
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
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
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
                        },
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
                        },
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
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
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
                        },
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
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
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
                        },
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
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
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
                        },
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
            time_bound = Utils.extract_range_timestamps(analytic['timespan'])
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
                            },
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
        time_bound = Utils.extract_range_timestamps(analytic['timespan'])
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
                        },
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
