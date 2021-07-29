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

import argparse
import logging.config
import os
from typing import Optional

import sentry_sdk
from elasticsearch import Elasticsearch
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from memex_logging.common.dao.collector import DaoCollector
from memex_logging.common.log.logging import get_logging_configuration
from memex_logging.ws.ws import WsInterface


logging.config.dictConfig(get_logging_configuration("logger"))
logger = logging.getLogger("logger.ws.main")

sentry_logging = LoggingIntegration(
    level=logging.INFO,  # Capture info and above as breadcrumbs
    event_level=logging.ERROR  # Send errors as events
)

sentry_sdk.init(
    integrations=[FlaskIntegration()],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0
)


def init_ws(
        elasticsearch_host: str,
        elasticsearch_port: int,
        elasticsearch_user: Optional[str],
        elasticsearch_password: Optional[str]
        ) -> WsInterface:

    es = Elasticsearch([{'host': elasticsearch_host, 'port': elasticsearch_port}], http_auth=(elasticsearch_user, elasticsearch_password))
    dao_collector = DaoCollector.build_dao_collector(es)
    ws_interface = WsInterface(dao_collector, es)
    return ws_interface


def build_interface_from_env():
    ws_interface = init_ws(
        elasticsearch_host=os.getenv("EL_HOST", "localhost"),
        elasticsearch_port=int(os.getenv("EL_PORT", 9200)),
        elasticsearch_user=os.getenv("EL_USERNAME", None),
        elasticsearch_password=os.getenv("EL_PASSWORD", None),
    )

    return ws_interface


def build_production_app():
    ws_interface = build_interface_from_env()
    return ws_interface.get_application()


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-eh", "--el_host", type=str, default=os.getenv("EL_HOST", "localhost"), help="The elasticsearch host")
    arg_parser.add_argument("-ep", "--el_port", type=int, default=int(os.getenv("EL_PORT", 9200)), help="The elasticsearch port")
    arg_parser.add_argument("-eu", "--el_username", type=str, default=os.getenv("EL_USERNAME", None), help="The username to access elasticsearch")
    arg_parser.add_argument("-epw", "--el_password", type=str, default=os.getenv("EL_PASSWORD", None), help="The password to access elasticsearch")
    arg_parser.add_argument("-wh", "--ws_host", type=str, default=os.getenv("WS_HOST", "0.0.0.0"), help="The web service host")
    arg_parser.add_argument("-wp", "--ws_port", type=int, default=int(os.getenv("WS_PORT", 80)), help="The web service port")
    args = arg_parser.parse_args()

    ws = init_ws(args.el_host, args.el_port, args.el_username, args.el_password)

    try:
        ws.run_server(args.ws_host, args.ws_port)
    except KeyboardInterrupt:
        pass
