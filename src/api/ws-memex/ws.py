from elasticsearch import Elasticsearch
import logging

from flask import Flask
from flask_restful import Api


class WsInterface(object):

    def __init__(self, elastic: Elasticsearch, project_name: str) -> None:
        self._app = Flask(__name__)
        self._api = Api(app=self._app)
        self._init_modules(elastic, project_name)

    def _init_modules(self, elastic, project_name) -> None:
        from messages_api.main import MessageResourceBuilder
        active_routes = [
            (MessageResourceBuilder.routes(elastic, project_name), "/message"),
            # (LoggingResourceBuilder.routes(elastic), "/log")
        ]

        for module_routes, prefix in active_routes:
            for resource, path, args in module_routes:
                logging.debug("Installing route %s", prefix + path)
                self._api.add_resource(resource, prefix + path, resource_class_args=args)

    def run_server(self, host: str = "127.0.0.1", port: int = 5000):
        self._app.run(host="127.0.0.1", port=5000, debug=True)
