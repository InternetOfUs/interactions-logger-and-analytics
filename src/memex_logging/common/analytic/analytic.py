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
from wenet.common.interface.task_manager import TaskManagerInterface

from memex_logging.common.model.analytic import UserAnalytic, MessageAnalytic, TaskAnalytic, TransactionAnalytic, \
    ConversationAnalytic, DialogueAnalytic, BotAnalytic
from memex_logging.common.model.result import AnalyticResult, SegmentationAnalyticResult, \
    ConversationLengthAnalyticResult, ConversationPathAnalyticResult, Segmentation, ConversationLength, ConversationPath
from memex_logging.utils.utils import Utils


logger = logging.getLogger("logger.common.analytic.analytic")


class AnalyticComputation:

    @staticmethod
    def compute_u_total(analytic: UserAnalytic, es: Elasticsearch) -> AnalyticResult:
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        user_list = []

        for item in response['aggregations']['terms_count']['buckets']:
            user_list.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return AnalyticResult(response['aggregations']['type_count']['value'], user_list, "userId")

    @staticmethod
    def compute_u_active(analytic: UserAnalytic, es: Elasticsearch) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        user_list = []

        for item in response['aggregations']['terms_count']['buckets']:
            user_list.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return AnalyticResult(response['aggregations']['type_count']['value'], user_list, "userId")

    @staticmethod
    def compute_u_engaged(analytic: UserAnalytic, es: Elasticsearch) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        user_list = []

        for item in response['aggregations']['terms_count']['buckets']:
            user_list.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return AnalyticResult(response['aggregations']['type_count']['value'], user_list, "userId")

    @staticmethod
    def compute_u_new(analytic: UserAnalytic, es: Elasticsearch) -> AnalyticResult:
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
                "terms_count": {
                    "terms": {
                        "field": "userId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
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
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "lt": min_bound.isoformat()
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        users_out_period = []
        for item in response['aggregations']['terms_count']['buckets']:
            users_out_period.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        users_out_period = set(users_out_period)

        final_users = users_in_period - users_out_period

        return AnalyticResult(len(final_users), list(final_users), "userId")

    @staticmethod
    def compute_m_from_users(analytic: MessageAnalytic, es: Elasticsearch) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                "terms_count": {
                    "terms": {
                        "field": "userId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        user_list = []
        total_counter = 0
        for item in response['aggregations']['terms_count']['buckets']:
            user_list.append(item['key'])
            total_counter = total_counter + int(item['doc_count'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return  AnalyticResult(total_counter, user_list, "userId")

    @staticmethod
    def compute_m_segmentation(analytic: MessageAnalytic, es: Elasticsearch) -> SegmentationAnalyticResult:
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
                "terms_count": {
                    "terms": {
                        "field": "type.keyword",
                        "size": 5
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        type_counter = []
        for item in response['aggregations']['terms_count']['buckets']:
            type_counter.append(Segmentation(item['key'], item['doc_count']))

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `5` but the number of types is higher")

        return SegmentationAnalyticResult(type_counter, "type")

    @staticmethod
    def compute_r_segmentation(analytic: MessageAnalytic, es: Elasticsearch) -> SegmentationAnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                "terms_count": {
                    "terms": {
                        "field": "content.type.keyword",
                        "size": 10
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        type_counter = []
        for item in response['aggregations']['terms_count']['buckets']:
            type_counter.append(Segmentation(item['key'], item['doc_count']))

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `10` but the number of content types is higher")

        return SegmentationAnalyticResult(type_counter, "content.type")

    @staticmethod
    def compute_m_conversation(analytic: MessageAnalytic, es: Elasticsearch) -> AnalyticResult:
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
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        conversation_list = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            conversation_list.append(item['key'])
            total_len = total_len + 1

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        return AnalyticResult(total_len, conversation_list, "conversationId")

    @staticmethod
    def compute_m_from_bot(analytic: MessageAnalytic, es: Elasticsearch) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                "terms_count": {
                    "terms": {
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
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
                "terms_count": {
                    "terms": {
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        for item in response['aggregations']['terms_count']['buckets']:
            messages.append(item['key'])
            total_len = total_len + item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of notification messages is higher")

        return AnalyticResult(total_len, messages, "messageId")

    @staticmethod
    def compute_m_responses(analytic: MessageAnalytic, es: Elasticsearch) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                "terms_count": {
                    "terms": {
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            messages.append(item['key'])
            total_len = total_len + item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return AnalyticResult(total_len, messages, "messageId")

    @staticmethod
    def compute_m_notifications(analytic: MessageAnalytic, es: Elasticsearch) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                "terms_count": {
                    "terms": {
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            messages.append(item['key'])
            total_len = total_len + item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of notification messages is higher")

        return AnalyticResult(total_len, messages, "messageId")

    @staticmethod
    def compute_m_unhandled(analytic: MessageAnalytic, es: Elasticsearch) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                "terms_count": {
                    "terms": {
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                messages.append(item['key'])
                total_len = total_len + item['doc_count']

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of unhandled messages is higher")

        return AnalyticResult(total_len, messages, "messageId")

    @staticmethod
    def compute_task_t_total(analytic: TaskAnalytic, task_manager_interface: TaskManagerInterface) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
        tasks = task_manager_interface.get_tasks(analytic.project, min_bound, max_bound)
        return AnalyticResult(len(tasks), [task.task_id for task in tasks], "taskId")

    @staticmethod
    def compute_transaction_t_total(analytic: TransactionAnalytic, task_manager_interface: TaskManagerInterface) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
        transactions = task_manager_interface.get_transactions(analytic.project, min_bound, max_bound)
        return AnalyticResult(len(transactions), [transaction.id for transaction in transactions], "transactionId")

    @staticmethod
    def compute_c_total(analytic: ConversationAnalytic, es: Elasticsearch) -> AnalyticResult:
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
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        conversation_list = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            conversation_list.append(item['key'])
            total_len = total_len + 1

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        return AnalyticResult(total_len, conversation_list, "conversationId")

    @staticmethod
    def compute_c_new(analytic: ConversationAnalytic, es: Elasticsearch) -> AnalyticResult:
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
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
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
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "lt": min_bound.isoformat()
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        conv_out_period = []
        for item in response['aggregations']['terms_count']['buckets']:
            conv_out_period.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        conv_out_period = set(conv_out_period)

        final_conv = conv_in_period - conv_out_period

        return AnalyticResult(len(final_conv), list(final_conv), "conversationId")

    @staticmethod
    def compute_c_length(analytic: ConversationAnalytic, es: Elasticsearch) -> ConversationLengthAnalyticResult:
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
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        conversation_list = []
        total_len = 0
        for item in response['aggregations']['terms_count']['buckets']:
            conversation_list.append(ConversationLength(item['key'], item['doc_count']))
            total_len = total_len + 1

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        return ConversationLengthAnalyticResult(total_len, conversation_list, "length")

    @staticmethod
    def compute_c_path(analytic: ConversationAnalytic, es: Elasticsearch) -> ConversationPathAnalyticResult:
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
                "terms_count": {
                    "terms": {
                        "field": "conversationId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        conversation_list = []
        for item in response['aggregations']['terms_count']['buckets']:
            conversation_list.append(item['key'])

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        conversation_list = list(set(conversation_list))
        paths = []

        for item in conversation_list:
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
                    "terms_count": {
                        "terms": {
                            "field": "messageId.keyword",
                            "size": 65535
                        }
                    }
                }
            }

            index = Utils.generate_index(data_type="message", project=analytic.project)
            response = es.search(index=index, body=body, size=0)
            message_list = []
            for obj in response['aggregations']['terms_count']['buckets']:
                message_list.append(obj['key'])

            paths.append(ConversationPath(item, message_list))

            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of messages is higher")

        return ConversationPathAnalyticResult(len(paths), paths, "path")

    @staticmethod
    def compute_d_fallback(analytic: DialogueAnalytic, es: Elasticsearch) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
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
                        "field": "messageId.keyword"
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        total_missed = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            total_missed = response['aggregations']['type_count']['value']

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
                        "field": "messageId.keyword"
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        total_messages = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            total_messages = response['aggregations']['type_count']['value']

        return AnalyticResult(total_missed, total_messages, "score")

    @staticmethod
    def compute_d_intents(analytic: DialogueAnalytic, es: Elasticsearch) -> AnalyticResult:
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
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

        return AnalyticResult(value, intent_list, "intent")

    @staticmethod
    def compute_d_domains(analytic: DialogueAnalytic, es: Elasticsearch) -> AnalyticResult:
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

        index = Utils.generate_index(data_type="message", project=analytic.project)
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

        return AnalyticResult(value, domain_list, "domain")

    @staticmethod
    def compute_b_response(analytic: BotAnalytic, es: Elasticsearch) -> AnalyticResult:
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
                        "field": "messageId.keyword"
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        total_messages = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in \
                response['aggregations']['type_count']:
            total_messages = response['aggregations']['type_count']['value']

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
                "terms_count": {
                    "terms": {
                        "field": "userId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = es.search(index=index, body=body, size=0)
        total_not_working = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in \
                response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                if item['doc_count'] == 1:
                    total_not_working = total_not_working + 1

        if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
            logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return AnalyticResult(total_not_working, total_messages, "score")
