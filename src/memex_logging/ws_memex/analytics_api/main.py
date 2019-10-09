import json

from flask import request, Response
from flask_restful import Resource, abort

# for handling elasticsearch
from elasticsearch import Elasticsearch


class AnalyticsResourceBuilder(object):
    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (AnalyticsPerformer, '/analytics', (es,)),
            (GetNoClickPerUser, '/analytics/usercount', (es,)),
            (GetNoClickPerEvent, '/analytics/eventcount', (es,))
        ]


class AnalyticsPerformer(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self):

        json_response = {
            "analyticId": "N_TEST IMIN",
            "status": "ok",
            "code": 200
        }

        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200

        return resp


class GetNoClickPerUser(Resource):

    def __init__(self, es:Elasticsearch):
        self._es = es

    def get(self):
        if 'userId' in request.args:
            response = self._es.search(index="memex-log*",body={"query": {"match": {"metadata.userId": request.args['userId']}}})
            if response['hits']['total']['value'] != 0:
                click_collection = {}
                # TODO check da qualche parte su namespace per capire se sto contando un evento
                for item in response['hits']['hits']:
                    if 'metadata' in item['_source']:
                        if 'eventId' in item['_source']['metadata']:
                            if item['_source']['metadata']['eventId'] in click_collection:
                                click_collection[item['_source']['metadata']['eventId']] = click_collection[
                                                                                             item['_source'][
                                                                                                 'metadata'][
                                                                                                 'eventId']] + 1
                            else:
                                click_collection[item['_source']['metadata']['eventId']] = 1

                json_response = {
                    "type": "click_per_user",
                    "user": request.args['userId'],
                    "click": click_collection,
                    "status": "ok",
                    "code": 200
                }
            else:
                json_response = {
                    "type": "click_per_user",
                    "user": request.args['userId'],
                    "click": {},
                    "status": "ok",
                    "code": 200
                }
        else:
            abort(500, message="a parameter userId is needed")
        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200

        return resp


class GetNoClickPerEvent(Resource):

    def __init__(self, es:Elasticsearch):
        self._es = es

    def get(self):
        if 'eventId' in request.args:
            response = self._es.search(index="memex-log*", body={"query": {"match": {"metadata.eventId": request.args['eventId']}}})
            if response['hits']['total']['value'] != 0:
                user_collection = {}
                for item in response['hits']['hits']:
                    if 'metadata' in item['_source']:
                        if 'userId' in item['_source']['metadata']:
                            if item['_source']['metadata']['userId'] in user_collection:
                                user_collection[item['_source']['metadata']['userId']] = user_collection[item['_source']['metadata']['userId']] + 1
                            else:
                                user_collection[item['_source']['metadata']['userId']] = 1

                json_response = {
                    "type": "click_per_event",
                    "event": request.args['eventId'],
                    "click": user_collection,
                    "status": "ok",
                    "code": 200
                }
            else:
                json_response = {
                    "type": "click_per_event",
                    "event": request.args['eventId'],
                    "click": {},
                    "status": "ok",
                    "code": 200
                }
        else:
            abort(500, message="a parameter userId is needed")

        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200

        return resp
