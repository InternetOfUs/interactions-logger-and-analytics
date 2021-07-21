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
from wenet.interface.wenet import WeNet

from memex_logging.celery import celery
from memex_logging.common.computation.analytic import AnalyticComputation
from memex_logging.common.model.analytic.analytic import Analytic
from memex_logging.common.model.time import MovingTimeWindow
from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.celery.analytic")


@celery.task(name='tasks.update_analytic')
def update_analytic(static_id: str):
    logger.info(f"Updating analytic with static id [{static_id}]")

    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    client = ApikeyClient(os.getenv("APIKEY"))
    wenet_interface = WeNet.build(client, platform_url=os.getenv("INSTANCE"))

    index_name = Utils.generate_index("analytic")
    raw_response = es.search(index=index_name, body={"query": {"match": {"staticId.keyword": static_id}}})

    if raw_response['hits']['total']['value'] == 0:
        logger.info(f"Analytic with static id [{static_id}] not found")
        raise ValueError(f"Analytic with static id [{static_id}] not found")

    index = raw_response['hits']['hits'][0]['_index']
    trace_id = raw_response['hits']['hits'][0]['_id']
    doc_type = raw_response['hits']['hits'][0]['_type']

    response = Analytic.from_repr(raw_response['hits']['hits'][0]['_source'])
    analytic_computation = AnalyticComputation(es, wenet_interface)
    response.result = analytic_computation.get_result(response.descriptor)
    es.index(index=index, id=trace_id, doc_type=doc_type, body=response.to_repr())
    logger.info("Result updated")


@celery.task(name='tasks.update_analytics')
def update_analytics():
    logger.info("Updating moving time window analytics")
    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    index_name = Utils.generate_index("analytic")
    results = scan(es, index=index_name, query={"query": {"match": {"query.timespan.type.keyword": MovingTimeWindow.moving_time_window_type()}}})

    for result in results:
        update_analytic.delay(result['_source']["staticId"])
