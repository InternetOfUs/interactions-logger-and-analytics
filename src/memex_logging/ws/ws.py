# Copyright 2020 U-Hopper srl
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

import celery
from elasticsearch import Elasticsearch
from flask import Flask
from flask_restful import Api

from memex_logging.ws.resource.analytics import AnalyticsResourceBuilder
from memex_logging.ws.resource.documentation import DocumentationResourceBuilder
from memex_logging.ws.resource.logging import LoggingResourceBuilder
from memex_logging.ws.resource.message import MessageResourceBuilder
from memex_logging.ws.resource.performance import PerformancesResourceBuilder


class WsInterface(object):

    def __init__(self, elastic: Elasticsearch) -> None:
        self._app = Flask("log-ws")
        self._app.config.update(
            CELERY_BROKER=os.getenv("CELERY_BROKER", None),
            CELERY_RESULT_BACKEND=os.getenv("CELERY_RESULT_BACKEND", None)
        )
        self._api = Api(app=self._app)
        self._init_modules(elastic)
        self.make_celery(self._app)

    def make_celery(self, app):
        celery_instance = celery.Celery(
            app.import_name,
            backend=app.config['CELERY_RESULT_BACKEND'],
            broker=app.config['CELERY_BROKER']
        )
        celery_instance.conf.update(app.config)

        class ContextTask(celery_instance.Task):
            def __call__(self, *args, **kwargs):
                with app.app_context():
                    return self.run(*args, **kwargs)

        celery.Task = ContextTask
        return celery_instance

    def _init_modules(self, elastic) -> None:
        active_routes = [
            (MessageResourceBuilder.routes(elastic), ""),
            (LoggingResourceBuilder.routes(elastic), ""),
            (PerformancesResourceBuilder.routes(elastic), "/performance"),
            (AnalyticsResourceBuilder.routes(elastic), ""),
            (DocumentationResourceBuilder.routes(), "")
        ]

        for module_routes, prefix in active_routes:
            for resource, path, args in module_routes:
                logging.debug("Installing route %s", prefix + path)
                self._api.add_resource(resource, prefix + path, resource_class_args=args)

    def run_server(self, host: str = "0.0.0.0", port: int = 80):
        self._app.run(host=host, port=port, debug=False)
