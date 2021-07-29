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

from memex_logging.common.model.analytic.descriptor.count import CountDescriptor, UserCountDescriptor, \
    MessageCountDescriptor, TaskCountDescriptor, TransactionCountDescriptor, ConversationCountDescriptor, \
    DialogueCountDescriptor, BotCountDescriptor
from memex_logging.common.model.analytic.result.count import CountResult
from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.common.analytic.analytic")


class CountComputation:

    def __init__(self, es: Elasticsearch, wenet_interface: WeNet) -> None:
        self.es = es
        self.wenet_interface = wenet_interface

    def get_result(self, analytic: CountDescriptor) -> CountResult:
        if isinstance(analytic, UserCountDescriptor):
            if analytic.metric.lower() == "total":
                result = self._total_users(analytic)
            elif analytic.metric.lower() == "active":
                result = self._active_users(analytic)
            elif analytic.metric.lower() == "engaged":
                result = self._engaged_users(analytic)
            elif analytic.metric.lower() == "new":
                result = self._new_users(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for UserCountDescriptor")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for UserCountDescriptor")

        elif isinstance(analytic, MessageCountDescriptor):
            if analytic.metric.lower() == "from_users":
                result = self._user_messages(analytic)
            elif analytic.metric.lower() == "from_bot":
                result = self._bot_messages(analytic)
            elif analytic.metric.lower() == "responses":
                result = self._response_messages(analytic)
            elif analytic.metric.lower() == "notifications":
                result = self._notification_messages(analytic)
            # elif analytic.metric.lower() == "unhandled":
            #     result = self._unhandled_messages(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for MessageCountDescriptor")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for MessageCountDescriptor")

        elif isinstance(analytic, TaskCountDescriptor):
            if analytic.metric.lower() == "total":
                result = self._total_tasks(analytic)
            elif analytic.metric.lower() == "active":
                result = self._active_tasks(analytic)
            elif analytic.metric.lower() == "closed":
                result = self._closed_tasks(analytic)
            elif analytic.metric.lower() == "new":
                result = self._new_tasks(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for TaskCountDescriptor")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for TaskCountDescriptor")

        elif isinstance(analytic, TransactionCountDescriptor):
            if analytic.metric.lower() == "total":
                result = self._total_transactions(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for TransactionCountDescriptor")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for TransactionCountDescriptor")

        elif isinstance(analytic, ConversationCountDescriptor):
            if analytic.metric.lower() == "total":
                result = self._total_conversations(analytic)
            elif analytic.metric.lower() == "new":
                result = self._new_conversations(analytic)
            # elif analytic.metric.lower() == "length":
            #     result = self._length_conversations(analytic)
            # elif analytic.metric.lower() == "path":
            #     result = self._path_conversations(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for ConversationCountDescriptor")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for ConversationCountDescriptor")

        elif isinstance(analytic, DialogueCountDescriptor):
            if analytic.metric.lower() == "fallback":
                result = self._fallback(analytic)
            elif analytic.metric.lower() == "intents":
                result = self._intents(analytic)
            elif analytic.metric.lower() == "domains":
                result = self._domains(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for DialogueCountDescriptor")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for DialogueCountDescriptor")

        elif isinstance(analytic, BotCountDescriptor):
            if analytic.metric.lower() == "response":
                result = self._bot_response(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for BotCountDescriptor")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for BotCountDescriptor")

        else:
            logger.info(f"Unrecognized class of CountDescriptor [{type(analytic)}]")
            raise ValueError(f"Unrecognized class of CountDescriptor [{type(analytic)}]")

        return result

    def _total_users(self, analytic: UserCountDescriptor) -> CountResult:
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
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        number_of_users = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            number_of_users = response['aggregations']['type_count']['value']

        return CountResult(number_of_users, datetime.now(), min_bound, max_bound)

    def _active_users(self, analytic: UserCountDescriptor) -> CountResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
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
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        number_of_users = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            number_of_users = response['aggregations']['type_count']['value']

        return CountResult(number_of_users, datetime.now(), min_bound, max_bound)

    def _engaged_users(self, analytic: UserCountDescriptor) -> CountResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
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
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        number_of_users = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            number_of_users = response['aggregations']['type_count']['value']

        return CountResult(number_of_users, datetime.now(), min_bound, max_bound)

    def _new_users(self, analytic: UserCountDescriptor) -> CountResult:
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

        return CountResult(len(final_users), datetime.now(), min_bound, max_bound)

    def _user_messages(self, analytic: MessageCountDescriptor) -> CountResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
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
        total_counter = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                total_counter = total_counter + int(item['doc_count'])

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return CountResult(total_counter, datetime.now(), min_bound, max_bound)

    def _bot_messages(self, analytic: MessageCountDescriptor) -> CountResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
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
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
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
                total_len = total_len + item['doc_count']

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of notification messages is higher")

        return CountResult(total_len, datetime.now(), min_bound, max_bound)

    def _response_messages(self, analytic: MessageCountDescriptor) -> CountResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
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
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                total_len = total_len + item['doc_count']

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of users is higher")

        return CountResult(total_len, datetime.now(), min_bound, max_bound)

    def _notification_messages(self, analytic: MessageCountDescriptor) -> CountResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
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
        total_len = 0
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                total_len = total_len + item['doc_count']

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `65535` but the number of notification messages is higher")

        return CountResult(total_len, datetime.now(), min_bound, max_bound)

    # def _unhandled_messages(self, analytic: MessageCountDescriptor) -> CountResult:
    #     min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
    #     body = {
    #         "query": {
    #             "bool": {
    #                 "must": [
    #                     {
    #                         "match": {
    #                             "handled.keyword": True
    #                         }
    #                     },
    #                     {
    #                         "match": {
    #                             "project.keyword": analytic.project
    #                         }
    #                     }
    #                 ],
    #                 "filter": [
    #                     {
    #                         "range": {
    #                             "timestamp": {
    #                                 "gte": min_bound.isoformat(),
    #                                 "lte": max_bound.isoformat()
    #                             }
    #                         }
    #                     }
    #                 ]
    #             }
    #         },
    #         "aggs": {
    #             "terms_count": {
    #                 "terms": {
    #                     "field": "messageId.keyword",
    #                     "size": 65535
    #                 }
    #             }
    #         }
    #     }
    #
    #     index = Utils.generate_index(data_type="message", project=analytic.project)
    #     response = self.es.search(index=index, body=body, size=0)
    #     total_len = 0
    #     if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
    #         for item in response['aggregations']['terms_count']['buckets']:
    #             total_len = total_len + item['doc_count']
    #
    #     if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
    #         if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
    #             logger.warning("The number of buckets is limited at `65535` but the number of unhandled messages is higher")
    #
    #     return CountResult(total_len, datetime.now(), min_bound, max_bound)

    def _total_tasks(self, analytic: TaskCountDescriptor) -> CountResult:
        """
        Compute the number of total tasks of a given application (analytic.project) in a given time range (analytic.timespan).
        The computation of that count is done summing up:
        * the number of tasks created up to the end of the time range that are still open;
        * the number of tasks created up to the end of the time range that are closed after the end of the time range;
        * the number of tasks closed in the time range.
        """
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        tasks = []
        tasks.extend(self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, creation_to=max_bound, has_close_ts=False))
        tasks.extend(self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, creation_to=max_bound, has_close_ts=True, closed_from=max_bound))
        tasks.extend(self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, has_close_ts=True, closed_from=min_bound, closed_to=max_bound))
        return CountResult(len(tasks), datetime.now(), min_bound, max_bound)

    def _active_tasks(self, analytic: TaskCountDescriptor) -> CountResult:
        """
        Compute the number of tasks of a given application (analytic.project) active at the end of a given time range (analytic.timespan).
        The computation of that count is done summing up:
        * the number of tasks created up to the end of the time range that are still open;
        * the number of tasks created up to the end of the time range that are closed after the end of the time range.
        """
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        tasks = []
        tasks.extend(self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, creation_to=max_bound, has_close_ts=False))
        tasks.extend(self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, creation_to=max_bound, has_close_ts=True, closed_from=max_bound))
        return CountResult(len(tasks), datetime.now(), min_bound, max_bound)

    def _closed_tasks(self, analytic: TaskCountDescriptor) -> CountResult:
        """
        Compute the number of tasks of a given application (analytic.project) closed in a given time range (analytic.timespan).
        The computation of that count is the number of tasks closed in the time range.
        """
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        tasks = self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, has_close_ts=True, closed_from=min_bound, closed_to=max_bound)
        return CountResult(len(tasks), datetime.now(), min_bound, max_bound)

    def _new_tasks(self, analytic: TaskCountDescriptor) -> CountResult:
        """
        Compute the number of new tasks of a given application (analytic.project) created in a given time range (analytic.timespan).
        The computation of that count is the number of tasks created in the time range.
        """
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        tasks = self.wenet_interface.task_manager.get_all_tasks(app_id=analytic.project, creation_from=min_bound, creation_to=max_bound)
        return CountResult(len(tasks), datetime.now(), min_bound, max_bound)

    def _total_transactions(self, analytic: TransactionCountDescriptor) -> CountResult:
        """
        Compute the number of total transactions of a given application (analytic.project) in a given time range (analytic.timespan).
        The computation of that count, since transactions do not have the concept of closed, is simply the number of transactions created in the time range.
        Optionally if specified a task identifier the transactions are only relative to that task.
        """
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        transactions = self.wenet_interface.task_manager.get_all_transactions(app_id=analytic.project, creation_from=min_bound, creation_to=max_bound,  task_id=analytic.task_id)
        return CountResult(len(transactions), datetime.now(), min_bound, max_bound)

    def _total_conversations(self, analytic: ConversationCountDescriptor) -> CountResult:
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
                        "field": "conversationId.keyword"
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        total_len = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            total_len = response['aggregations']['type_count']['value']

        return CountResult(total_len, datetime.now(), min_bound, max_bound)

    def _new_conversations(self, analytic: ConversationCountDescriptor) -> CountResult:
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

        return CountResult(len(final_conv), datetime.now(), min_bound, max_bound)

    # def _length_conversations(self, analytic: ConversationCountDescriptor) -> ConversationLengthCountResult:
    #     min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
    #     body = {
    #         "query": {
    #             "bool": {
    #                 "must": [
    #                     {
    #                         "match": {
    #                             "project.keyword": analytic.project
    #                         }
    #                     }
    #                 ],
    #                 "filter": [
    #                     {
    #                         "range": {
    #                             "timestamp": {
    #                                 "gte": min_bound.isoformat(),
    #                                 "lte": max_bound.isoformat()
    #                             }
    #                         }
    #                     }
    #                 ]
    #             }
    #         },
    #         "aggs": {
    #             "terms_count": {
    #                 "terms": {
    #                     "field": "conversationId.keyword",
    #                     "size": 65535
    #                 }
    #             }
    #         }
    #     }
    #
    #     index = Utils.generate_index(data_type="message", project=analytic.project)
    #     response = self.es.search(index=index, body=body, size=0)
    #     conversation_list = []
    #     total_len = 0
    #     if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
    #         for item in response['aggregations']['terms_count']['buckets']:
    #             conversation_list.append(ConversationLength(item['key'], item['doc_count']))
    #             total_len = total_len + 1
    #
    #     if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
    #         if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
    #             logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")
    #
    #     return ConversationLengthCountResult(total_len, conversation_list, datetime.now(), min_bound, max_bound)
    #
    # def _path_conversations(self, analytic: ConversationCountDescriptor) -> ConversationPathCountResult:
    #     min_bound, max_bound = Utils.extract_range_timestamps(analytic.timespan)
    #     body = {
    #         "query": {
    #             "bool": {
    #                 "must": [
    #                     {
    #                         "match": {
    #                             "project.keyword": analytic.project
    #                         }
    #                     }
    #                 ],
    #                 "filter": [
    #                     {
    #                         "range": {
    #                             "timestamp": {
    #                                 "gte": min_bound.isoformat(),
    #                                 "lte": max_bound.isoformat()
    #                             }
    #                         }
    #                     }
    #                 ]
    #             }
    #         },
    #         "aggs": {
    #             "terms_count": {
    #                 "terms": {
    #                     "field": "conversationId.keyword",
    #                     "size": 65535
    #                 }
    #             }
    #         }
    #     }
    #
    #     index = Utils.generate_index(data_type="message", project=analytic.project)
    #     response = self.es.search(index=index, body=body, size=0)
    #     conversation_list = []
    #     if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
    #         for item in response['aggregations']['terms_count']['buckets']:
    #             conversation_list.append(item['key'])
    #
    #     if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
    #         if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
    #             logger.warning("The number of buckets is limited at `65535` but the number of conversations is higher")
    #
    #     conversation_list = list(set(conversation_list))
    #     paths = []
    #
    #     for item in conversation_list:
    #         body = {
    #             "query": {
    #                 "bool": {
    #                     "must": [
    #                         {
    #                             "match": {
    #                                 "conversationId.keyword": item
    #                             }
    #                         },
    #                         {
    #                             "match": {
    #                                 "project.keyword": analytic.project
    #                             }
    #                         }
    #                     ],
    #                     "filter": [
    #                         {
    #                             "range": {
    #                                 "timestamp": {
    #                                     "gte": min_bound.isoformat(),
    #                                     "lte": max_bound.isoformat()
    #                                 }
    #                             }
    #                         }
    #                     ]
    #                 }
    #             },
    #             "aggs": {
    #                 "terms_count": {
    #                     "terms": {
    #                         "field": "messageId.keyword",
    #                         "size": 65535
    #                     }
    #                 }
    #             }
    #         }
    #
    #         index = Utils.generate_index(data_type="message", project=analytic.project)
    #         response = self.es.search(index=index, body=body, size=0)
    #         message_list = []
    #         if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
    #             for obj in response['aggregations']['terms_count']['buckets']:
    #                 message_list.append(obj['key'])
    #
    #             paths.append(ConversationPath(item, message_list))
    #
    #         if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
    #             if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
    #                 logger.warning("The number of buckets is limited at `65535` but the number of messages is higher")
    #
    #     return ConversationPathCountResult(len(paths), paths, datetime.now(), min_bound, max_bound)

    def _fallback(self, analytic: DialogueCountDescriptor) -> CountResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
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

        return CountResult(total_missed, datetime.now(), min_bound, max_bound)

    def _intents(self, analytic: DialogueCountDescriptor) -> CountResult:
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
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        value = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return CountResult(value, datetime.now(), min_bound, max_bound)

    def _domains(self, analytic: DialogueCountDescriptor) -> CountResult:
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
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        value = 0
        if 'aggregations' in response and 'type_count' in response['aggregations'] and 'value' in response['aggregations']['type_count']:
            value = response['aggregations']['type_count']['value']

        return CountResult(value, datetime.now(), min_bound, max_bound)

    def _bot_response(self, analytic: BotCountDescriptor) -> CountResult:
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

        return CountResult(total_not_working, datetime.now(), min_bound, max_bound)
