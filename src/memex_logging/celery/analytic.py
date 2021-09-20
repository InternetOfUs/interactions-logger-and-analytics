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
from memex_logging.common.model.analytic.time import FixedTimeWindow


logger = logging.getLogger("logger.celery.analytic")


@celery.task(name='tasks.update_analytic')
def update_analytic(analytic_id: str):
    logger.info(f"Updating analytic with id [{analytic_id}]")

    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    dao_collector = DaoCollector.build_dao_collector(es)
    analytic = dao_collector.analytic.get(analytic_id)

    client = ApikeyClient(os.getenv("APIKEY"))
    wenet_interface = WeNet.build(client, platform_url=os.getenv("INSTANCE"))
    analytic_computation = AnalyticComputation(es, wenet_interface)
    analytic.result = analytic_computation.get_result(analytic.descriptor)
    dao_collector.analytic.update(analytic)
    logger.info(f"Result of analytic with id [{analytic_id}] updated")


@celery.task(name='tasks.update_analytics')
def update_analytics(time_window_type: Optional[str] = None):
    logger.info(f"Updating {time_window_type if time_window_type is not None else 'all'} analytics")
    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    dao_collector = DaoCollector.build_dao_collector(es)
    analytics = dao_collector.analytic.list(time_window_type=time_window_type)

    for analytic in analytics:
        update_analytic.delay(analytic.analytic_id)


@celery.task(name='tasks.update_not_concluded_fixed_time_window_analytics')
def update_not_concluded_fixed_time_window_analytics():
    logger.info(f"Updating not concluded fixed time window analytics")
    es = Elasticsearch([{'host': os.getenv("EL_HOST", "localhost"), 'port': int(os.getenv("EL_PORT", 9200))}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))
    dao_collector = DaoCollector.build_dao_collector(es)
    analytics = dao_collector.analytic.list(time_window_type=FixedTimeWindow.type())

    for analytic in analytics:
        if analytic.result is None or (isinstance(analytic.descriptor.time_span, FixedTimeWindow) and analytic.descriptor.time_span.end > analytic.result.creation_datetime):
            update_analytic.delay(analytic.analytic_id)
