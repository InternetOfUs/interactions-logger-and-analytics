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

from elasticsearch import Elasticsearch
import logging

from flask import Flask
from flask_restful import Api

from memex_logging.ws_memex.documentation_api.main import DocumentationResourceBuilder
from memex_logging.ws_memex.messages_api.main import MessageResourceBuilder
from memex_logging.ws_memex.logging_api.main import LoggingResourceBuilder
from memex_logging.ws_memex.performances_api.main import PerformancesResourceBuilder
from memex_logging.ws_memex.analytics_api.main import  AnalyticsResourceBuilder


class WsInterface(object):

    def __init__(self, elastic: Elasticsearch) -> None:
        self._app = Flask(__name__)
        self._api = Api(app=self._app)
        self._init_modules(elastic)

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
