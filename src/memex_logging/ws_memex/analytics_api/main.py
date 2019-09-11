import json

from flask import request, Response
from flask_restful import Resource
# for second-level logging
import logging
# for handling elasticsearch
from elasticsearch import Elasticsearch

from memex_logging.models.log import Log
from memex_logging.utils.utils import Utils


class AnalyticsResourceBuilder(object):
    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (AnalyticsPerformer, '/perform', (es,))
        ]


class AnalyticsPerformer(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self):
        Utils.get_mapping(self._es, "memex")
