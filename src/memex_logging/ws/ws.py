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
from flask import Flask
from flask_restful import Api

from memex_logging.common.dao.collector import DaoCollector
from memex_logging.ws.resource.analytic import AnalyticsResourceBuilder
from memex_logging.ws.resource.documentation import DocumentationResourceBuilder
from memex_logging.ws.resource.logging import LoggingResourceBuilder
from memex_logging.ws.resource.message import MessageResourceBuilder
from memex_logging.ws.resource.performance import PerformancesResourceBuilder


logger = logging.getLogger("logger.ws.ws")


class WsInterface(object):

    def __init__(self, dao_collector: DaoCollector, es: Elasticsearch) -> None:
        self._dao_collector = dao_collector
        self._es = es

        self._app = Flask("logger-ws")
        self._app.config.update(
            CELERY_RESULT_BACKEND=os.getenv("CELERY_RESULT_BACKEND", None),
            CELERY_BROKER_URL=os.getenv("CELERY_BROKER_URL", None)
        )
        self._api = Api(app=self._app)
        self._init_modules(self._dao_collector, self._es)

    def _init_modules(self, dao_collector: DaoCollector, es: Elasticsearch) -> None:
        active_routes = [
            (MessageResourceBuilder.routes(dao_collector), ""),
            (LoggingResourceBuilder.routes(es), ""),
            (PerformancesResourceBuilder.routes(es), "/performance"),
            (AnalyticsResourceBuilder.routes(es), ""),
            (DocumentationResourceBuilder.routes(), "")
        ]

        for module_routes, prefix in active_routes:
            for resource, path, args in module_routes:
                logger.debug("Installing route %s", prefix + path)
                self._api.add_resource(resource, prefix + path, resource_class_args=args)

    def run_server(self, host: str = "0.0.0.0", port: int = 80):
        self._app.run(host=host, port=port, debug=False)

    def get_application(self):
        return self._app

    @staticmethod
    def init_celery(celery, app):
        celery.conf.update(app.config)
        TaskBase = celery.Task

        class ContextTask(TaskBase):
            def __call__(self, *args, **kwargs):
                with app.app_context():
                    return self.run(*args, **kwargs)

        celery.Task = ContextTask
