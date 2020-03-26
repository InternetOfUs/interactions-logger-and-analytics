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

from __future__ import absolute_import

import argparse

from elasticsearch import Elasticsearch

from memex_logging.ws_memex.ws import WsInterface

if __name__ == '__main__':

    argParser = argparse.ArgumentParser()
    argParser.add_argument("-hs", "--host", type=str, dest="h", default="localhost")
    argParser.add_argument("-p", "--port", type=str, dest="p", default="9200")
    args = argParser.parse_args()

    es = Elasticsearch([{'host': args.h, 'port': args.p}])

    ws = WsInterface(es)

    try:
        ws.run_server()
    except KeyboardInterrupt:
        pass
