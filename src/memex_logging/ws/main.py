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

import argparse
import logging.config
import os
from typing import Optional

from elasticsearch import Elasticsearch

from memex_logging.common.dao.collector import DaoCollector
from memex_logging.common.log.logging import get_logging_configuration
from memex_logging.ws.ws import WsInterface


logging.config.dictConfig(get_logging_configuration("logger"))
logger = logging.getLogger("logger.ws.main")


def init_ws(
        elasticsearch_host: str,
        elasticsearch_port: int,
        elasticsearch_user: Optional[str],
        elasticsearch_password: Optional[str]
        ) -> WsInterface:

    es = Elasticsearch([{'host': elasticsearch_host, 'port': elasticsearch_port}], http_auth=(elasticsearch_user, elasticsearch_password))
    dao_collector = DaoCollector.build_dao_collector(es)
    ws = WsInterface(dao_collector, es)
    return ws


def build_production_app():
    ws_interface = init_ws(
        elasticsearch_host=os.getenv("EL_HOST", "localhost"),
        elasticsearch_port=int(os.getenv("EL_PORT", 9200)),
        elasticsearch_user=os.getenv("EL_USERNAME", None),
        elasticsearch_password=os.getenv("EL_PASSWORD", None),
    )

    return ws_interface.get_application()


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-eh", "--ehost", type=str, dest="ehost", default=os.getenv("EL_HOST", "localhost"), help="The elasticsearch host")
    arg_parser.add_argument("-ep", "--eport", type=int, dest="eport", default=int(os.getenv("EL_PORT", 9200)), help="The elasticsearch port")
    arg_parser.add_argument("-eu", "--euser", type=str, dest="euser", default=os.getenv("EL_USERNAME", None), help="The username to access elasticsearch")
    arg_parser.add_argument("-ew", "--epw", type=str, dest="epw", default=os.getenv("EL_PASSWORD", None), help="The password to access elasticsearch")
    arg_parser.add_argument("-wh", "--whost", type=str, dest="whost", default=os.getenv("WS_HOST", "0.0.0.0"), help="The web service host")
    arg_parser.add_argument("-wp", "--wport", type=int, dest="wport", default=int(os.getenv("WS_PORT", 80)), help="The web service port")
    args = arg_parser.parse_args()

    ws = init_ws(args.ehost, args.eport, args.euser, args.epw)

    try:
        ws.run_server(args.whost, args.wport)
    except KeyboardInterrupt:
        pass
