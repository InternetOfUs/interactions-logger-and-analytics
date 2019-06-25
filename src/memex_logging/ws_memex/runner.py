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
