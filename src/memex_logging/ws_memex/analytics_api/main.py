import json

from flask import request, Response
from flask_restful import Resource, abort

# for handling elasticsearch
from elasticsearch import Elasticsearch

import logging
from memex_logging.analytic.analytic import AnalyticComputation


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
        args = request.args
        id = args['id']
        if id == "" or id is None:
            abort(500, message="ANALYTIC.GET: invalid identifier")

        response = self._es.search(index="analytic*", body={"query": {"match": {"_id": id}}})

        if response['hits']['total'] == 0:
            abort(404, message="resource not found")
        else:
            return response['hits']['hits'][0]['_source'], 200

    def post(self):
        analytic = request.json

        # Data model check on the fly. Call the function and decide whether it is valid or not.

        # Object to be stored: query + results inside of `query` and `result`
        if AnalyticComputation.analytic_validity_check(analytic):
            metric = str(analytic['metric']).lower()
            answer = None
            ac = AnalyticComputation()
            if metric == "u:total":
                answer = ac.compute_u_total(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "u:active":
                answer = ac.compute_u_active(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "u:engaged":
                answer = ac.compute_u_engaged(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "u:new":
                answer = ac.compute_u_new(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }

            elif metric == "m:from_user":
                answer = ac.compute_m_from_user(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "m:conversation":
                answer = ac.compute_m_conversation(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "m:from_bot":
                answer = ac.compute_m_from_bot(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "m:responses":
                answer = ac.compute_m_responses(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "m:notifications":
                answer = ac.compute_m_notifications(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "m:unhandled":
                answer = ac.compute_m_unhandled(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "m:notification_engagement":
                answer = ac.compute_m_notification_engagement(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "c:total":
                answer = ac.compute_c_total(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "c:new":
                answer = ac.compute_c_new(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "c:length":
                answer = ac.compute_c_length(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "c:path":
                answer = ac.compute_c_path(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "d:fallback":
                answer = ac.compute_d_fallback(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "d:interrupted":
                answer = ac.compute_d_interrupted(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "d:intents":
                answer = ac.compute_d_intents(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "d:domains":
                answer = ac.compute_d_domains(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "b:retention":
                answer = ac.compute_b_retention(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            elif metric == "b:response":
                answer = ac.compute_b_response(analytic, self._es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "users": answer[1]
                    },
                    "status": 200
                }
            
        # save analytics and put the _id to retrieve it 

        index_name = "analytic-" + analytic['project']
        query = self._es.index(index=index_name, doc_type='_doc', body=json_response)

        json_response['id'] = query['_id']
        resp = Response(json.dumps(json_response), mimetype='application/json')
        resp.status_code = 200

        return resp


class GetNoClickPerUser(Resource):

    def __init__(self, es:Elasticsearch):
        self._es = es

    def get(self):
        if 'userId' in request.args:
            response = self._es.search(index="logging-memex*",body={"query": {"match": {"metadata.userId": request.args['userId']}}})
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
            response = self._es.search(index="logging-memex*", body={"query": {"match": {"metadata.eventId": request.args['eventId']}}})
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


class GetUsers(Resource):

    def __init__(self, es:Elasticsearch):
        self._es = es

    def get(self):
        response = self._es.search(index="memex-log*", body={"query": {"match": {}}})
        print(response)

