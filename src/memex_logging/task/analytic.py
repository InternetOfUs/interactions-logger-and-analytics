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
import os

from elasticsearch import Elasticsearch
from wenet.common.interface.client import ApikeyClient
from wenet.common.interface.task_manager import TaskManagerInterface

from memex_logging.common.analytic.aggregation import AggregationComputation
from memex_logging.common.analytic.analytic import AnalyticComputation
from memex_logging.common.analytic.builder import AnalyticBuilder
from memex_logging.common.model.aggregation import Aggregation
from memex_logging.common.model.analytic import UserAnalytic, MessageAnalytic, TaskAnalytic, ConversationAnalytic, \
    DialogueAnalytic, BotAnalytic, CommonAnalytic, TransactionAnalytic
from memex_logging.ws import celery


logger = logging.getLogger("logger.task.analytic")


@celery.task()
def compute_analytic(analytic: dict, static_id: str):
    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    client = ApikeyClient(os.getenv("APIKEY"))
    task_manager_interface = TaskManagerInterface(client, instance=os.getenv("INSTANCE"))

    try:
        analytic = AnalyticBuilder.from_repr(analytic)
    except ValueError as e:
        logger.info(f"Analytic not valid. {e.args[0]}")

    if isinstance(analytic, CommonAnalytic):
        if isinstance(analytic, UserAnalytic):
            if analytic.metric.lower() == "u:total":
                answer = AnalyticComputation.compute_u_total(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "u:active":
                answer = AnalyticComputation.compute_u_active(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "u:engaged":
                answer = AnalyticComputation.compute_u_engaged(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "u:new":
                answer = AnalyticComputation.compute_u_new(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "staticId": static_id
                }
            else:
                logger.info("User metric value not valid")
                return

        elif isinstance(analytic, MessageAnalytic):
            if analytic.metric.lower() == "m:from_users":
                answer = AnalyticComputation.compute_m_from_users(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "m:segmentation":
                answer = AnalyticComputation.compute_m_segmentation(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "counts": answer,
                        "type": "type"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "r:segmentation":
                answer = AnalyticComputation.compute_r_segmentation(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "counts": answer,
                        "type": "content.type"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "m:conversation":
                answer = AnalyticComputation.compute_m_conversation(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "m:from_bot":
                answer = AnalyticComputation.compute_m_from_bot(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "m:responses":
                answer = AnalyticComputation.compute_m_responses(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "m:notifications":
                answer = AnalyticComputation.compute_m_notifications(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "m:unhandled":
                answer = AnalyticComputation.compute_m_unhandled(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "staticId": static_id
                }
            else:
                logger.info("Message metric value not valid")
                return

        elif isinstance(analytic, TaskAnalytic):
            if analytic.metric.lower() == "t:total":  # TODO also for "t:active", "t:closed", "t:new"
                answer = AnalyticComputation.compute_task_t_total(analytic, task_manager_interface)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "taskId"
                    },
                    "staticId": static_id
                }
            else:
                logger.info("Task metric value not valid")
                return

        elif isinstance(analytic, TransactionAnalytic):
            if analytic.metric.lower() == "t:total":  # TODO also for "t:new", "t:segmentation"
                answer = AnalyticComputation.compute_transaction_t_total(analytic, task_manager_interface)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "transactionId"
                    },
                    "staticId": static_id
                }
            else:
                logger.info("Transaction metric value not valid")
                return

        elif isinstance(analytic, ConversationAnalytic):
            if analytic.metric.lower() == "c:total":
                answer = AnalyticComputation.compute_c_total(analytic, es)
                json_response = {
                    "query": analytic,
                    "conversations": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "c:new":
                answer = AnalyticComputation.compute_c_new(analytic, es)
                json_response = {
                    "query": analytic,
                    "conversations": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "c:length":
                answer = AnalyticComputation.compute_c_length(analytic, es)
                json_response = {
                    "query": analytic,
                    "conversations": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "c:path":
                answer = AnalyticComputation.compute_c_path(analytic, es)
                json_response = {
                    "query": analytic,
                    "conversations": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            else:
                logger.info("Conversation metric value not valid")
                return

        elif isinstance(analytic, DialogueAnalytic):
            if analytic.metric.lower() == "d:fallback":
                answer = AnalyticComputation.compute_d_fallback(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "d:intents":
                answer = AnalyticComputation.compute_d_intents(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "intent"
                    },
                    "staticId": static_id
                }
            elif analytic.metric.lower() == "d:domains":
                answer = AnalyticComputation.compute_d_domains(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "domain"
                    },
                    "staticId": static_id
                }
            else:
                logger.info("Dialogue metric value not valid")
                return

        elif isinstance(analytic, BotAnalytic):
            if analytic.metric.lower() == "b:response":
                answer = AnalyticComputation.compute_b_response(analytic, es)
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "score"
                    },
                    "staticId": static_id
                }
            else:
                logger.info("Bot metric value not valid")
                return

        else:
            logger.info("Analytic not valid")
            return

        project = analytic.project
        index_name = "analytic-" + project.lower() + "-" + analytic.dimension.lower()
        es.index(index=index_name, doc_type='_doc', body=json_response)
        logger.info("Result stored in " + str(index_name))

    elif isinstance(analytic, Aggregation):
        if analytic.aggregation.lower() == "max":
            answer = AggregationComputation.max_aggr(analytic, es)
            json_response = {
                "query": analytic,
                "result": {
                    "max": answer
                },
                "staticId": static_id
            }
        elif analytic.aggregation.lower() == "min":
            answer = AggregationComputation.min_aggr(analytic, es)
            json_response = {
                "query": analytic,
                "result": {
                    "min": answer
                },
                "staticId": static_id
            }
        elif analytic.aggregation.lower() == "avg":
            answer = AggregationComputation.avg_aggr(analytic, es)
            json_response = {
                "query": analytic,
                "result": {
                    "avg": answer
                },
                "staticId": static_id
            }
        elif analytic.aggregation.lower() == "stats":
            answer = AggregationComputation.stats_aggr(analytic, es)
            json_response = {
                "query": analytic,
                "result": {
                    "stats": answer
                },
                "staticId": static_id
            }
        elif analytic.aggregation.lower() == "sum":
            answer = AggregationComputation.sum_aggr(analytic, es)
            json_response = {
                "query": analytic,
                "result": {
                    "sum": answer
                },
                "staticId": static_id
            }
        elif analytic.aggregation.lower() == "value_count":
            answer = AggregationComputation.value_count_aggr(analytic, es)
            json_response = {
                "query": analytic,
                "result": {
                    "value_count": answer
                },
                "staticId": static_id
            }
        elif analytic.aggregation.lower() == "cardinality":
            answer = AggregationComputation.cardinality_aggr(analytic, es)
            json_response = {
                "query": analytic,
                "result": {
                    "cardinality": answer
                },
                "staticId": static_id
            }
        elif analytic.aggregation.lower() == "extended_stats":
            answer = AggregationComputation.extended_stats_aggr(analytic, es)
            json_response = {
                "query": analytic,
                "result": {
                    "extended_stats": answer
                },
                "staticId": static_id
            }
        elif analytic.aggregation.lower() == "percentiles":
            answer = AggregationComputation.percentiles_aggr(analytic, es)
            json_response = {
                "query": analytic,
                "result": {
                    "percentiles": answer
                },
                "staticId": static_id
            }
        else:
            logger.info("Aggregation value not valid")
            return

        project = analytic.project
        index_name = "analytic-" + project.lower() + "-" + analytic.aggregation.lower()
        es.index(index=index_name, doc_type='_doc', body=json_response)
        logger.info("Result stored in " + str(index_name))

    else:
        logger.info("Type not valid, not an analytic nor an aggregation")
        return
