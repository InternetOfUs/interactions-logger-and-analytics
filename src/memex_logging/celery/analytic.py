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
from typing import Optional

from elasticsearch import Elasticsearch
from wenet.interface.client import ApikeyClient
from wenet.interface.wenet import WeNet

from memex_logging.celery import celery
from memex_logging.common.computation.analytic import AnalyticComputation
from memex_logging.common.dao.collector import DaoCollector


logger = logging.getLogger("logger.celery.analytic")


@celery.task(name='tasks.update_analytic')
def update_analytic(analytic_id: str):
    logger.info(f"Updating analytic with id [{analytic_id}]")

    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    dao_collector = DaoCollector.build_dao_collector(es)
    analytic, trace_id, index, doc_type = dao_collector.analytic.get_with_additional_information(analytic_id)

    client = ApikeyClient(os.getenv("APIKEY"))
    wenet_interface = WeNet.build(client, platform_url=os.getenv("INSTANCE"))
    analytic_computation = AnalyticComputation(es, wenet_interface)
    analytic.result = analytic_computation.get_result(analytic.descriptor)
    dao_collector.analytic.update(index, trace_id, analytic, doc_type)
    logger.info(f"Result of analytic with id [{analytic_id}] updated")


@celery.task(name='tasks.update_analytics')
def update_analytics(time_window_type: Optional[str] = None):
    logger.info("Updating all analytics")
    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    dao_collector = DaoCollector.build_dao_collector(es)
    analytics = dao_collector.analytic.list(time_window_type=time_window_type)

    for analytic in analytics:
        update_analytic.delay(analytic.analytic_id)
