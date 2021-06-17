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

from memex_logging.common.analytic.aggregation import AggregationComputation
from memex_logging.common.analytic.analytic import AnalyticComputation
from memex_logging.common.model.aggregation import Aggregation
from memex_logging.common.model.analytic import CommonAnalytic
from memex_logging.ws import celery


logger = logging.getLogger("logger.task.analytic")


@celery.task()
def compute_analytic(analytic: dict, static_id: str):
    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    if str(analytic['type']).lower() == CommonAnalytic.ANALYTIC_TYPE:
        if AnalyticComputation.analytic_validity_check(analytic):
            metric = str(analytic['metric']).lower()
            if metric == "u:total":
                answer = AnalyticComputation.compute_u_total(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "staticId": static_id
                }
            elif metric == "u:active":
                answer = AnalyticComputation.compute_u_active(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "staticId": static_id
                }
            elif metric == "u:engaged":
                answer = AnalyticComputation.compute_u_engaged(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "staticId": static_id
                }
            elif metric == "u:new":
                answer = AnalyticComputation.compute_u_new(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "staticId": static_id
                }
            elif metric == "m:from_users":
                answer = AnalyticComputation.compute_m_from_users(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "staticId": static_id
                }
            elif metric == "m:segmentation":
                answer = AnalyticComputation.compute_m_segmentation(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "counts": answer,
                        "type": "type"
                    },
                    "staticId": static_id
                }
            elif metric == "r:segmentation":
                answer = AnalyticComputation.compute_r_segmentation(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "counts": answer,
                        "type": "content.type"
                    },
                    "staticId": static_id
                }
            elif metric == "m:conversation":
                answer = AnalyticComputation.compute_m_conversation(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            elif metric == "m:from_bot":
                answer = AnalyticComputation.compute_m_from_bot(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "staticId": static_id
                }
            elif metric == "m:responses":
                answer = AnalyticComputation.compute_m_responses(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "staticId": static_id
                }
            elif metric == "m:notifications":
                answer = AnalyticComputation.compute_m_notifications(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "staticId": static_id
                }
            elif metric == "m:unhandled":
                answer = AnalyticComputation.compute_m_unhandled(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "staticId": static_id
                }
            elif metric == "c:total":
                answer = AnalyticComputation.compute_c_total(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "conversations": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            elif metric == "c:new":
                answer = AnalyticComputation.compute_c_new(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "conversations": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            elif metric == "c:length":
                answer = AnalyticComputation.compute_c_length(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "conversations": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "object"
                    },
                    "staticId": static_id
                }
            elif metric == "c:path":
                answer = AnalyticComputation.compute_c_path(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "conversations": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            elif metric == "d:fallback":
                answer = AnalyticComputation.compute_d_fallback(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "staticId": static_id
                }
            elif metric == "d:intents":
                answer = AnalyticComputation.compute_d_intents(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "intent"
                    },
                    "staticId": static_id
                }
            elif metric == "d:domains":
                answer = AnalyticComputation.compute_d_domains(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "domain"
                    },
                    "staticId": static_id
                }
            elif metric == "b:response":
                answer = AnalyticComputation.compute_b_response(analytic, es, analytic['project'])
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
                logger.info("Metric value not valid")
                return

            project = analytic['project']
            index_name = "analytic-" + project.lower() + "-" + analytic['dimension']
            es.index(index=index_name, doc_type='_doc', body=json_response)
            logger.info("Result stored in " + str(index_name))

        else:
            logger.info("Analytic not valid")
            return

    elif str(analytic['type']).lower() == Aggregation.AGGREGATION_TYPE:
        if AggregationComputation.aggregation_validity_check(analytic):
            aggregation = str(analytic['aggregation']).lower()
            if aggregation == "max":
                answer = AggregationComputation.max_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "max": answer
                    },
                    "staticId": static_id
                }
            elif aggregation == "min":
                answer = AggregationComputation.min_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "min": answer
                    },
                    "staticId": static_id
                }
            elif aggregation == "avg":
                answer = AggregationComputation.avg_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "avg": answer
                    },
                    "staticId": static_id
                }
            elif aggregation == "stats":
                answer = AggregationComputation.stats_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "stats": answer
                    },
                    "staticId": static_id
                }
            elif aggregation == "sum":
                answer = AggregationComputation.sum_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "sum": answer
                    },
                    "staticId": static_id
                }
            elif aggregation == "value_count":
                answer = AggregationComputation.value_count_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "value_count": answer
                    },
                    "staticId": static_id
                }
            elif aggregation == "cardinality":
                answer = AggregationComputation.cardinality_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "cardinality": answer
                    },
                    "staticId": static_id
                }
            elif aggregation == "extended_stats":
                answer = AggregationComputation.extended_stats_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "extended_stats": answer
                    },
                    "staticId": static_id
                }
            elif aggregation == "percentiles":
                answer = AggregationComputation.percentiles_aggr(analytic, es, analytic['project'])
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

            project = analytic['project']
            index_name = "analytic-" + project.lower() + "-" + analytic['aggregation']
            es.index(index=index_name, doc_type='_doc', body=json_response)
            logger.info("Result stored in " + str(index_name))

        else:
            logger.info("Analytic not valid")
            return

    else:
        logger.info("Type not valid, not an analytic nor an aggregation")
        return
