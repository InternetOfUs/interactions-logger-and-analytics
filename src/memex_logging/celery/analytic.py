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
from wenet.interface.client import ApikeyClient
from wenet.interface.task_manager import TaskManagerInterface

from memex_logging.common.analytic.aggregation import AggregationComputation
from memex_logging.common.analytic.analytic import AnalyticComputation
from memex_logging.common.analytic.builder import AnalyticBuilder
from memex_logging.common.model.aggregation import AggregationAnalytic
from memex_logging.common.model.analytic import UserAnalytic, MessageAnalytic, TaskAnalytic, ConversationAnalytic, \
    DialogueAnalytic, BotAnalytic, DimensionAnalytic, TransactionAnalytic
from memex_logging.common.model.response import AnalyticResponse, AggregationResponse
from memex_logging.celery import celery


logger = logging.getLogger("logger.task.analytic")


@celery.task()
def compute_analytic(analytic: dict, static_id: str):
    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    client = ApikeyClient(os.getenv("APIKEY"))
    task_manager_interface = TaskManagerInterface(client, os.getenv("INSTANCE"))
    logger.info("Computing analytic: " + str(analytic))

    try:
        analytic = AnalyticBuilder.from_repr(analytic)
    except ValueError as e:
        logger.info(f"Analytic not valid. {e.args[0]}")

    if isinstance(analytic, DimensionAnalytic):
        if isinstance(analytic, UserAnalytic):
            if analytic.metric.lower() == "u:total":
                result = AnalyticComputation.compute_u_total(analytic, es)
            elif analytic.metric.lower() == "u:active":
                result = AnalyticComputation.compute_u_active(analytic, es)
            elif analytic.metric.lower() == "u:engaged":
                result = AnalyticComputation.compute_u_engaged(analytic, es)
            elif analytic.metric.lower() == "u:new":
                result = AnalyticComputation.compute_u_new(analytic, es)
            else:
                logger.info("User metric value not valid")
                return

        elif isinstance(analytic, MessageAnalytic):
            if analytic.metric.lower() == "m:from_users":
                result = AnalyticComputation.compute_m_from_users(analytic, es)
            elif analytic.metric.lower() == "m:segmentation":
                result = AnalyticComputation.compute_m_segmentation(analytic, es)
            elif analytic.metric.lower() == "r:segmentation":
                result = AnalyticComputation.compute_r_segmentation(analytic, es)
            elif analytic.metric.lower() == "m:from_bot":
                result = AnalyticComputation.compute_m_from_bot(analytic, es)
            elif analytic.metric.lower() == "m:responses":
                result = AnalyticComputation.compute_m_responses(analytic, es)
            elif analytic.metric.lower() == "m:notifications":
                result = AnalyticComputation.compute_m_notifications(analytic, es)
            elif analytic.metric.lower() == "m:unhandled":
                result = AnalyticComputation.compute_m_unhandled(analytic, es)
            else:
                logger.info("Message metric value not valid")
                return

        elif isinstance(analytic, TaskAnalytic):
            if analytic.metric.lower() == "t:total":
                result = AnalyticComputation.compute_task_t_total(analytic, task_manager_interface)
            elif analytic.metric.lower() == "t:active":
                result = AnalyticComputation.compute_task_t_active(analytic, task_manager_interface)
            elif analytic.metric.lower() == "t:closed":
                result = AnalyticComputation.compute_task_t_closed(analytic, task_manager_interface)
            elif analytic.metric.lower() == "t:new":
                result = AnalyticComputation.compute_task_t_new(analytic, task_manager_interface)
            else:
                logger.info("Task metric value not valid")
                return

        elif isinstance(analytic, TransactionAnalytic):
            if analytic.metric.lower() == "t:total":
                result = AnalyticComputation.compute_transaction_t_total(analytic, task_manager_interface)
            elif analytic.metric.lower() == "t:segmentation":
                result = AnalyticComputation.compute_transaction_t_segmentation(analytic, task_manager_interface)
            else:
                logger.info("Transaction metric value not valid")
                return

        elif isinstance(analytic, ConversationAnalytic):
            if analytic.metric.lower() == "c:total":
                result = AnalyticComputation.compute_c_total(analytic, es)
            elif analytic.metric.lower() == "c:new":
                result = AnalyticComputation.compute_c_new(analytic, es)
            elif analytic.metric.lower() == "c:length":
                result = AnalyticComputation.compute_c_length(analytic, es)
            elif analytic.metric.lower() == "c:path":
                result = AnalyticComputation.compute_c_path(analytic, es)
            else:
                logger.info("Conversation metric value not valid")
                return

        elif isinstance(analytic, DialogueAnalytic):
            if analytic.metric.lower() == "d:fallback":
                result = AnalyticComputation.compute_d_fallback(analytic, es)
            elif analytic.metric.lower() == "d:intents":
                result = AnalyticComputation.compute_d_intents(analytic, es)
            elif analytic.metric.lower() == "d:domains":
                result = AnalyticComputation.compute_d_domains(analytic, es)
            else:
                logger.info("Dialogue metric value not valid")
                return

        elif isinstance(analytic, BotAnalytic):
            if analytic.metric.lower() == "b:response":
                result = AnalyticComputation.compute_b_response(analytic, es)
            else:
                logger.info("Bot metric value not valid")
                return

        else:
            logger.info("Analytic not valid")
            return

        project = analytic.project
        index_name = "analytic-" + project.lower() + "-" + analytic.dimension.lower()
        es.index(index=index_name, doc_type='_doc', body=AnalyticResponse(analytic, result, static_id).to_repr())
        logger.info("Result stored in " + str(index_name))

    elif isinstance(analytic, AggregationAnalytic):
        if analytic.aggregation.lower() == "max":
            result = AggregationComputation.max(analytic, es)
        elif analytic.aggregation.lower() == "min":
            result = AggregationComputation.min(analytic, es)
        elif analytic.aggregation.lower() == "avg":
            result = AggregationComputation.avg(analytic, es)
        elif analytic.aggregation.lower() == "stats":
            result = AggregationComputation.stats(analytic, es)
        elif analytic.aggregation.lower() == "sum":
            result = AggregationComputation.sum(analytic, es)
        elif analytic.aggregation.lower() == "value_count":
            result = AggregationComputation.value_count(analytic, es)
        elif analytic.aggregation.lower() == "cardinality":
            result = AggregationComputation.cardinality(analytic, es)
        elif analytic.aggregation.lower() == "extended_stats":
            result = AggregationComputation.extended_stats(analytic, es)
        elif analytic.aggregation.lower() == "percentiles":
            result = AggregationComputation.percentiles(analytic, es)
        else:
            logger.info("Aggregation value not valid")
            return

        project = analytic.project
        index_name = "analytic-" + project.lower() + "-" + analytic.aggregation.lower()
        es.index(index=index_name, doc_type='_doc', body=AggregationResponse(analytic, result, static_id).to_repr())
        logger.info("Result stored in " + str(index_name))

    else:
        logger.info("Type not valid, not an analytic nor an aggregation")
        return
