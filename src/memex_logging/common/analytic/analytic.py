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

from elasticsearch import Elasticsearch
from wenet.interface.wenet import WeNet

from memex_logging.common.model.analytic import UserAnalytic, MessageAnalytic, TaskAnalytic, TransactionAnalytic, \
    ConversationAnalytic, DialogueAnalytic, BotAnalytic, DimensionAnalytic
from memex_logging.common.model.result import AnalyticResult, SegmentationAnalyticResult, \
    ConversationLengthAnalyticResult, ConversationPathAnalyticResult, Segmentation, ConversationLength, \
    ConversationPath, TransactionAnalyticResult, TransactionReturn, CommonResult
from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.common.analytic.analytic")


class AnalyticComputation:

    def __init__(self, es: Elasticsearch, wenet_interface: WeNet) -> None:
        self.es = es
        self.wenet_interface = wenet_interface

    def get_analytic_result(self, analytic: DimensionAnalytic) -> CommonResult:
        if isinstance(analytic, UserAnalytic):
            if analytic.metric.lower() == "u:total":
                result = self._total_users(analytic)
            elif analytic.metric.lower() == "u:active":
                result = self._active_users(analytic)
            elif analytic.metric.lower() == "u:engaged":
                result = self._engaged_users(analytic)
            elif analytic.metric.lower() == "u:new":
                result = self._new_users(analytic)
            elif analytic.metric.lower() == "a:segmentation":
                result = self._user_age_segmentation(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for UserAnalytic")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for UserAnalytic")

        elif isinstance(analytic, MessageAnalytic):
            if analytic.metric.lower() == "m:from_users":
                result = self._user_messages(analytic)
            elif analytic.metric.lower() == "m:segmentation":
                result = self._messages_segmentation(analytic)
            elif analytic.metric.lower() == "u:segmentation":
                result = self._user_messages_segmentation(analytic)
            elif analytic.metric.lower() == "m:from_bot":
                result = self._bot_messages(analytic)
            elif analytic.metric.lower() == "m:responses":
                result = self._response_messages(analytic)
            elif analytic.metric.lower() == "m:notifications":
                result = self._notification_messages(analytic)
            elif analytic.metric.lower() == "m:unhandled":
                result = self._unhandled_messages(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for MessageAnalytic")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for MessageAnalytic")

        elif isinstance(analytic, TaskAnalytic):
            if analytic.metric.lower() == "t:total":
                result = self._total_tasks(analytic)
            elif analytic.metric.lower() == "t:active":
                result = self._active_tasks(analytic)
            elif analytic.metric.lower() == "t:closed":
                result = self._closed_tasks(analytic)
            elif analytic.metric.lower() == "t:new":
                result = self._new_tasks(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for TaskAnalytic")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for TaskAnalytic")

        elif isinstance(analytic, TransactionAnalytic):
            if analytic.metric.lower() == "t:total":
                result = self._total_transactions(analytic)
            elif analytic.metric.lower() == "t:segmentation":
                result = self._transactions_segmentation(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for TransactionAnalytic")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for TransactionAnalytic")

        elif isinstance(analytic, ConversationAnalytic):
            if analytic.metric.lower() == "c:total":
                result = self._total_conversations(analytic)
            elif analytic.metric.lower() == "c:new":
                result = self._new_conversations(analytic)
            elif analytic.metric.lower() == "c:length":
                result = self._length_conversations(analytic)
            elif analytic.metric.lower() == "c:path":
                result = self._path_conversations(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for ConversationAnalytic")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for ConversationAnalytic")

        elif isinstance(analytic, DialogueAnalytic):
            if analytic.metric.lower() == "d:fallback":
                result = self._fallback(analytic)
            elif analytic.metric.lower() == "d:intents":
                result = self._intents(analytic)
            elif analytic.metric.lower() == "d:domains":
                result = self._domains(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for DialogueAnalytic")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for DialogueAnalytic")

        elif isinstance(analytic, BotAnalytic):
            if analytic.metric.lower() == "b:response":
                result = self._bot_response(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for BotAnalytic")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for BotAnalytic")

        else:
            logger.info(f"Unrecognized class of analytic [{type(analytic)}]")
            raise ValueError(f"Unrecognized class of analytic [{type(analytic)}]")

        return result

    def _total_users(self, analytic: UserAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        user_list = []
        number_of_users = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                user_list.append(item['key'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            number_of_users = response['aggregations']['type_count']['value']

        return AnalyticResult(number_of_users, user_list, "userId", datetime.now(), min_bound, max_bound)

    def _active_users(self, analytic: UserAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        user_list = []
        number_of_users = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                user_list.append(item['key'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            number_of_users = response['aggregations']['type_count']['value']

        return AnalyticResult(number_of_users, user_list, "userId", datetime.now(), min_bound, max_bound)

    def _engaged_users(self, analytic: UserAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        user_list = []
        number_of_users = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                user_list.append(item['key'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            number_of_users = response['aggregations']['type_count']['value']

        return AnalyticResult(number_of_users, user_list, "userId", datetime.now(), min_bound, max_bound)

    def _new_users(self, analytic: UserAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        users_in_period = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                users_in_period.append(item['key'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
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
        response = self.es.search(index=index, body=body, size=0)
        users_out_period = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                users_out_period.append(item['key'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        users_out_period = set(users_out_period)

        final_users = users_in_period - users_out_period

        return AnalyticResult(len(final_users), list(final_users), "userId", datetime.now(), min_bound, max_bound)

    def _user_messages(self, analytic: MessageAnalytic) -> AnalyticResult:
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
                        "field": "messageId.keyword",
                        "size": 65535
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        messages = []
        total_counter = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                messages.append(item['key'])
                total_counter = total_counter + int(item['doc_count'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return AnalyticResult(total_counter, messages, "messageId", datetime.now(), min_bound, max_bound)

    def _user_age_segmentation(self, analytic: UserAnalytic) -> SegmentationAnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
        user_ids = self.wenet_interface.hub.get_user_ids_for_app(analytic.project, from_datetime=min_bound, to_datetime=max_bound)
        ages = []
        for user_id in user_ids:
            user_profile = self.wenet_interface.profile_manager.get_user_profile(user_id)
            ages.append(Utils.compute_age(user_profile.date_of_birth.date_dt) if user_profile.date_of_birth is not None and user_profile.date_of_birth.date_dt is not None else None)

        type_counter = [
            Segmentation("0-18", len([age for age in ages if age is not None and 0 <= age <= 18])),
            Segmentation("19-25", len([age for age in ages if age is not None and 19 <= age <= 25])),
            Segmentation("26-35", len([age for age in ages if age is not None and 26 <= age <= 35])),
            Segmentation("36-45", len([age for age in ages if age is not None and 36 <= age <= 45])),
            Segmentation("46-55", len([age for age in ages if age is not None and 46 <= age <= 55])),
            Segmentation("55+", len([age for age in ages if age is not None and 55 < age])),
            Segmentation("unavailable", ages.count(None))
        ]
        return SegmentationAnalyticResult(type_counter, datetime.now(), min_bound, max_bound)

    def _messages_segmentation(self, analytic: MessageAnalytic) -> SegmentationAnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        type_counter = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                type_counter.append(Segmentation(item['key'], item['doc_count']))

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `5` but the number of types is higher")

        return SegmentationAnalyticResult(type_counter, datetime.now(), min_bound, max_bound)

    def _user_messages_segmentation(self, analytic: MessageAnalytic) -> SegmentationAnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        type_counter = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                type_counter.append(Segmentation(item['key'], item['doc_count']))

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `10` but the number of content types is higher")

        return SegmentationAnalyticResult(type_counter, datetime.now(), min_bound, max_bound)

    def _bot_messages(self, analytic: MessageAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                messages.append(item['key'])
                total_len = total_len + item['doc_count']

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
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
        response = self.es.search(index=index, body=body, size=0)
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                messages.append(item['key'])
                total_len = total_len + item['doc_count']

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of notification messages is higher")

        return AnalyticResult(total_len, messages, "messageId", datetime.now(), min_bound, max_bound)

    def _response_messages(self, analytic: MessageAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                messages.append(item['key'])
                total_len = total_len + item['doc_count']

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return AnalyticResult(total_len, messages, "messageId", datetime.now(), min_bound, max_bound)

    def _notification_messages(self, analytic: MessageAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                messages.append(item['key'])
                total_len = total_len + item['doc_count']

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of notification messages is higher")

        return AnalyticResult(total_len, messages, "messageId", datetime.now(), min_bound, max_bound)

    def _unhandled_messages(self, analytic: MessageAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        messages = []
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                messages.append(item['key'])
                total_len = total_len + item['doc_count']

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of unhandled messages is higher")

        return AnalyticResult(total_len, messages, "messageId", datetime.now(), min_bound, max_bound)

    def _total_tasks(self, analytic: TaskAnalytic) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
        tasks = []
        tasks.extend(self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, creation_to=max_bound, has_close_ts=False))
        tasks.extend(self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, creation_to=max_bound, has_close_ts=True, closed_from=max_bound))
        tasks.extend(self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, has_close_ts=True, closed_from=min_bound, closed_to=max_bound))
        return AnalyticResult(len(tasks), [task.task_id for task in tasks], "taskId", datetime.now(), min_bound, max_bound)

    def _active_tasks(self, analytic: TaskAnalytic) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
        tasks = []
        tasks.extend(self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, creation_to=max_bound, has_close_ts=False))
        tasks.extend(self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, creation_to=max_bound, has_close_ts=True, closed_from=max_bound))
        return AnalyticResult(len(tasks), [task.task_id for task in tasks], "taskId", datetime.now(), min_bound, max_bound)

    def _closed_tasks(self, analytic: TaskAnalytic) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
        tasks = self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, has_close_ts=True, closed_from=min_bound, closed_to=max_bound)
        return AnalyticResult(len(tasks), [task.task_id for task in tasks], "taskId", datetime.now(), min_bound, max_bound)

    def _new_tasks(self, analytic: TaskAnalytic) -> AnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
        tasks = self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, creation_from=min_bound, creation_to=max_bound)
        return AnalyticResult(len(tasks), [task.task_id for task in tasks], "taskId", datetime.now(), min_bound, max_bound)

    def _total_transactions(self, analytic: TransactionAnalytic) -> TransactionAnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
        transactions = self.wenet_interface.task_manager.get_all_transactions(app_id=analytic.project, creation_from=min_bound, creation_to=max_bound,  task_id=analytic.task_id)
        task_ids = set([transaction.task_id for transaction in transactions])
        transaction_returns = [TransactionReturn(task_id, [transaction.id for transaction in transactions if transaction.task_id == task_id]) for task_id in task_ids]
        return TransactionAnalyticResult(len(transactions), transaction_returns, datetime.now(), min_bound, max_bound)

    def _transactions_segmentation(self, analytic: TransactionAnalytic) -> SegmentationAnalyticResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
        transactions = self.wenet_interface.task_manager.get_all_transactions(app_id=analytic.project, creation_from=min_bound, creation_to=max_bound,  task_id=analytic.task_id)
        transaction_labels = [transaction.label for transaction in transactions]
        unique_labels = set(transaction_labels)
        type_counter = [Segmentation(label, transaction_labels.count(label)) for label in unique_labels]
        return SegmentationAnalyticResult(type_counter, datetime.now(), min_bound, max_bound)

    def _total_conversations(self, analytic: ConversationAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        conversation_list = []
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                conversation_list.append(item['key'])
                total_len = total_len + 1

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        return AnalyticResult(total_len, conversation_list, "conversationId", datetime.now(), min_bound, max_bound)

    def _new_conversations(self, analytic: ConversationAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        conv_in_period = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                conv_in_period.append(item['key'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
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
        response = self.es.search(index=index, body=body, size=0)
        conv_out_period = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                conv_out_period.append(item['key'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        conv_out_period = set(conv_out_period)

        final_conv = conv_in_period - conv_out_period

        return AnalyticResult(len(final_conv), list(final_conv), "conversationId", datetime.now(), min_bound, max_bound)

    def _length_conversations(self, analytic: ConversationAnalytic) -> ConversationLengthAnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        conversation_list = []
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                conversation_list.append(ConversationLength(item['key'], item['doc_count']))
                total_len = total_len + 1

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")

        return ConversationLengthAnalyticResult(total_len, conversation_list, datetime.now(), min_bound, max_bound)

    def _path_conversations(self, analytic: ConversationAnalytic) -> ConversationPathAnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        conversation_list = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                conversation_list.append(item['key'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
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
            response = self.es.search(index=index, body=body, size=0)
            message_list = []
            if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
                for obj in response['aggregations']['terms_count']['buckets']:
                    message_list.append(obj['key'])

                paths.append(ConversationPath(item, message_list))

            if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
                if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                    logger.warning("The number of buckets is limited at `65535` but the number of messages is higher")

        return ConversationPathAnalyticResult(len(paths), paths, datetime.now(), min_bound, max_bound)

    def _fallback(self, analytic: DialogueAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
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
        response = self.es.search(index=index, body=body, size=0)
        total_messages = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            total_messages = response['aggregations']['type_count']['value']

        return AnalyticResult(total_missed, total_messages, "score", datetime.now(), min_bound, max_bound)

    def _intents(self, analytic: DialogueAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        intent_list = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                intent_list.append(item['key'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of intents is higher")

        value = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return AnalyticResult(value, intent_list, "intent", datetime.now(), min_bound, max_bound)

    def _domains(self, analytic: DialogueAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        domain_list = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                domain_list.append(item['key'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of domains is higher")

        value = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return AnalyticResult(value, domain_list, "domain", datetime.now(), min_bound, max_bound)

    def _bot_response(self, analytic: BotAnalytic) -> AnalyticResult:
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
        response = self.es.search(index=index, body=body, size=0)
        total_messages = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
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
        response = self.es.search(index=index, body=body, size=0)
        total_not_working = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                if item['doc_count'] == 1:
                    total_not_working = total_not_working + 1

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return AnalyticResult(total_not_working, total_messages, "score", datetime.now(), min_bound, max_bound)
