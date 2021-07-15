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
from elasticsearch.helpers import scan
from wenet.interface.client import ApikeyClient
from wenet.interface.task_manager import TaskManagerInterface

from memex_logging.celery import celery
from memex_logging.common.analytic.aggregation import AggregationComputation
from memex_logging.common.analytic.analytic import AnalyticComputation
from memex_logging.common.analytic.builder import AnalyticBuilder
from memex_logging.common.model.aggregation import AggregationAnalytic
from memex_logging.common.model.analytic import DimensionAnalytic
from memex_logging.common.model.response import AnalyticResponse, AggregationResponse
from memex_logging.common.model.time import MovingTimeWindow
from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.celery.analytic")


@celery.task(name='tasks.update_analytic')
def update_analytic(static_id: str):
    logger.info(f"Updating analytic with static id [{static_id}]")

    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    client = ApikeyClient(os.getenv("APIKEY"))
    task_manager_interface = TaskManagerInterface(client, os.getenv("INSTANCE"))

    index_name = Utils.generate_index("analytic")
    raw_response = es.search(index=index_name, body={"query": {"match": {"staticId.keyword": static_id}}})

    if raw_response['hits']['total']['value'] == 0:
        logger.info(f"Analytic with static id [{static_id}] not found")
        raise ValueError(f"Analytic with static id [{static_id}] not found")

    trace_id = raw_response['hits']['hits'][0]['_id']
    analytic = AnalyticBuilder.from_repr(raw_response['hits']['hits'][0]['_source']["query"])

    if isinstance(analytic, DimensionAnalytic):
        response = AnalyticResponse.from_repr(raw_response['hits']['hits'][0]['_source'])
        analytic_computation = AnalyticComputation(es, task_manager_interface)
        response.result = analytic_computation.get_analytic_result(analytic)
        project = analytic.project
        index_name = "analytic-" + project.lower() + "-" + analytic.dimension.lower()
        es.index(index=index_name, id=trace_id, doc_type='_doc', body=response.to_repr())
        logger.info("Result updated")

    elif isinstance(analytic, AggregationAnalytic):
        response = AggregationResponse.from_repr(raw_response['hits']['hits'][0]['_source'])
        aggregation_computation = AggregationComputation(es)
        response.result = aggregation_computation.get_aggregation_result(analytic)
        project = analytic.project
        index_name = "analytic-" + project.lower() + "-" + analytic.aggregation.lower()
        es.index(index=index_name, id=trace_id, doc_type='_doc', body=response.to_repr())
        logger.info("Result updated")

    else:
        logger.info(f"Unrecognized class of analytic [{type(analytic)}]")
        raise ValueError(f"Unrecognized class of analytic [{type(analytic)}]")


@celery.task(name='tasks.update_analytics')
def update_analytics():
    logger.info("Updating moving time window analytics")
    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    index_name = Utils.generate_index("analytic")
    results = scan(es, index=index_name, query={"query": {"match": {"query.timespan.type.keyword": MovingTimeWindow.moving_time_window_type()}}})

    for result in results:
        update_analytic.delay(result['_source']["staticId"])
