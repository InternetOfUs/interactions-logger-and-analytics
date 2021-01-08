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
import os

from elasticsearch import Elasticsearch

from memex_logging.ws_memex.ws import WsInterface


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-eh", "--ehost", type=str, dest="ehost", default=os.getenv("EL_HOST", "localhost"))
    arg_parser.add_argument("-ep", "--eport", type=int, dest="eport", default=int(os.getenv("EL_PORT", 9200)))
    arg_parser.add_argument("-wh", "--whost", type=str, dest="whost", default=os.getenv("WS_HOST", "0.0.0.0"))
    arg_parser.add_argument("-wp", "--wport", type=int, dest="wport", default=int(os.getenv("WS_PORT", 80)))
    args = arg_parser.parse_args()

    es = Elasticsearch([{'host': args.ehost, 'port': args.eport}], http_auth=(os.getenv("EL_USERNAME", None), os.getenv("EL_PASSWORD", None)))

    ws = WsInterface(es)

    try:
        ws.run_server(args.whost, args.wport)
    except KeyboardInterrupt:
        pass
