import json

from flask import request, Response
from flask_restful import Resource
# for second-level logging
import logging
# for handling elasticsearch
from elasticsearch import Elasticsearch

from memex_logging.models.analytics import Analytic
from memex_logging.models.log import Log
from memex_logging.utils.utils import Utils


class AnalyticsResourceBuilder(object):
    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (AnalyticsPerformer, '/analytics', (es,))
        ]


class AnalyticsPerformer(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def post(self):
        item = request.json
        temp_analytic = Analytic.from_rep(item)
        index_name = "memex-analytics"
        query = self._es.index(index=index_name, doc_type='_doc', body=temp_analytic.to_repr())
        id = query['_id']

        json_response = {
            "analyticId": id,
            "status": "ok",
            "code": 200
        }

        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200

        return resp
