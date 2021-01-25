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

import json
import uuid

import celery
from elasticsearch import Elasticsearch
from flask import request, Response
from flask_restful import Resource, abort

from memex_logging.common.analytic import AnalyticComputation
from memex_logging.common.analytic import AggregationComputation


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

        response = self._es.search(index="analytic*", body={"query": {"match": {"static_id.keyword": id}}})

        if response['hits']['total'] == 0:
            abort(404, message="resource not found")
        else:
            return response['hits']['hits'][0]['_source'], 200

    def post(self):
        static_id = uuid.uuid4()
        analytic = request.json
        self._compute(analytic, static_id)
        return static_id, 200

    @celery.task()
    def _compute(self, analytic, static_id):
        if 'type' not in analytic:
            abort(500, message="Type must be specified")
        elif str(analytic['type']).lower() == "analytic":

            # Data model check on the fly. Call the function and decide whether it is valid or not.

            # Object to be stored: query + results inside of `query` and `result`
            if AnalyticComputation.analytic_validity_check(analytic):
                metric = str(analytic['metric']).lower()
                json_response = {}
                ac = AnalyticComputation()
                if metric == "u:total":
                    answer = ac.compute_u_total(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "userId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "u:active":
                    answer = ac.compute_u_active(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "userId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "u:engaged":
                    answer = ac.compute_u_engaged(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "userId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "u:new":
                    answer = ac.compute_u_new(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "userId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }

                elif metric == "m:from_user":
                    answer = ac.compute_m_from_user(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "userId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                    print(answer[0])
                    print(answer[1])
                elif metric == "m:conversation":
                    answer = ac.compute_m_conversation(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "conversationId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "m:from_bot":
                    answer = ac.compute_m_from_bot(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "messageId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                    print(answer[0])
                    print(answer[1])
                elif metric == "m:responses":
                    answer = ac.compute_m_responses(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "messageId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "m:notifications":
                    answer = ac.compute_m_notifications(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "messageId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "m:unhandled":
                    answer = ac.compute_m_unhandled(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "messageId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "m:notification_engagement":
                    answer = ac.compute_m_notification_engagement(analytic, self._es, analytic['project'])  # does not exists
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "messageId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "c:total":
                    answer = ac.compute_c_total(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "conversationId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "c:new":
                    answer = ac.compute_c_new(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "conversationId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "c:length":
                    answer = ac.compute_c_length(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "conversations": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "object"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "c:path":
                    answer = ac.compute_c_path(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "conversationId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "d:fallback":
                    answer = ac.compute_d_fallback(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "conversationId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "d:interrupted":
                    answer = ac.compute_d_interrupted(analytic, self._es, analytic['project'])  # does not exists
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "conversationId"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "d:intents":
                    answer = ac.compute_d_intents(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "intent"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "d:domains":
                    answer = ac.compute_d_domains(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "domain"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "b:retention":
                    answer = ac.compute_b_retention(analytic, self._es, analytic['project'])  # does not exists
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "score"
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "b:response":
                    answer = ac.compute_b_response(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "count": answer[0],
                            "items": answer[1],
                            "type": "score"
                        },
                        "status": 200,
                        "static_id": static_id
                    }

            # save analytics and put the _id to retrieve it

            index_name = "analytic-" + analytic['project'] + "-" + analytic['dimension']
            query = self._es.index(index=index_name, doc_type='_doc', body=json_response)

            json_response['id'] = query['_id']
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 200

            return resp
        elif str(analytic['type']).lower() == "aggregation":
            if AggregationComputation.aggregation_validity_check(analytic):
                metric = str(analytic['aggregation']).lower()
                json_response = {}
                ac = AggregationComputation()
                if metric == "max":
                    answer = ac.max_aggr(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "max": answer
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "min":
                    answer = ac.min_aggr(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "min": answer
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "avg":
                    answer = ac.avg_aggr(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "avg": answer
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "stats":
                    answer = ac.stats_aggr(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "stats": answer
                        },
                        "stats": 200,
                        "static_id": static_id
                    }
                elif metric == "sum":
                    answer = ac.sum_aggr(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "sum": answer
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "value_count":
                    answer = ac.value_count_aggr(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "value_count": answer
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "cardinality":
                    answer = ac.cardinality_aggr(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "cardinality": answer
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "extended_stats":
                    answer = ac.extended_stats_aggr(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "extended_stats": answer
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "percentiles":
                    answer = ac.percentiles_aggr(analytic, self._es, analytic['project'])
                    json_response = {
                        "query": analytic,
                        "result": {
                            "percentiles": answer
                        },
                        "status": 200,
                        "static_id": static_id
                    }
                elif metric == "percentile_ranks":
                    answer = ac.percentile_ranks_aggr(analytic, self._es, analytic['project'])  # does not exists
                    json_response = {
                        "query": analytic,
                        "result": {
                            "percentile_ranks": answer
                        },
                        "status": 200,
                        "static_id": static_id
                    }

                index_name = "analytic-" + analytic['project'] + "-" + analytic['aggregation']
                query = self._es.index(index=index_name, doc_type='_doc', body=json_response)

                json_response['id'] = query['_id']
                resp = Response(json.dumps(json_response), mimetype='application/json')
                resp.status_code = 200

                return resp
        else:
            abort(500, message="Invalid value for field type")


class GetNoClickPerUser(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self):
        if 'userId' in request.args:
            response = self._es.search(index="logging-memex*", body={"query": {"match": {"metadata.userId": request.args['userId']}}})
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

    def __init__(self, es: Elasticsearch):
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

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self):
        response = self._es.search(index="memex-log*", body={"query": {"match": {}}})
        print(response)
