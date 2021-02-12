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

from memex_logging.common.analytic.aggregation import AggregationComputation
from memex_logging.common.analytic.analytic import AnalyticComputation
from memex_logging.utils.utils import Utils


class AnalyticsResourceBuilder(object):
    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (AnalyticsPerformer, '/analytic', (es,)),
            (GetNoClickPerUser, '/analytic/usercount', (es,)),
            (GetNoClickPerEvent, '/analytic/eventcount', (es,))
        ]


class AnalyticsPerformer(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self):
        static_id = request.args.get('staticId')
        project = request.args.get('project')
        if static_id == "" or static_id is None:
            abort(400, message="invalid `staticId`")

        index_name = Utils.generate_index("analytic", project=project)
        response = self._es.search(index=index_name, body={"query": {"match": {"staticId.keyword": static_id}}})

        if response['hits']['total'] == 0:
            abort(404, message="resource not found")
        else:
            return response['hits']['hits'][0]['_source'], 200

    def post(self):
        static_id = str(uuid.uuid4())
        analytic = request.json
        compute(es=self._es, analytic=analytic, static_id=static_id)
        return {"staticId": static_id}, 200


@celery.task()
def compute(es: Elasticsearch, analytic: dict, static_id: str):
    if 'type' not in analytic:
        abort(400, message="`type` must be specified")
    elif str(analytic['type']).lower() == "analytic":

        # Data model check on the fly. Call the function and decide whether it is valid or not.

        # Object to be stored: query + results inside of `query` and `result`
        if AnalyticComputation.analytic_validity_check(analytic):
            metric = str(analytic['metric']).lower()
            json_response = {}
            ac = AnalyticComputation()
            if metric == "u:total":
                answer = ac.compute_u_total(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "u:active":
                answer = ac.compute_u_active(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "u:engaged":
                answer = ac.compute_u_engaged(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "u:new":
                answer = ac.compute_u_new(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "m:from_user":
                answer = ac.compute_m_from_user(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "userId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "m:conversation":
                answer = ac.compute_m_conversation(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "m:from_bot":
                answer = ac.compute_m_from_bot(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "m:responses":
                answer = ac.compute_m_responses(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "m:notifications":
                answer = ac.compute_m_notifications(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "m:unhandled":
                answer = ac.compute_m_unhandled(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "messageId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "c:total":
                answer = ac.compute_c_total(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "c:new":
                answer = ac.compute_c_new(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "c:length":
                answer = ac.compute_c_length(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "conversations": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "object"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "c:path":
                answer = ac.compute_c_path(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "d:fallback":
                answer = ac.compute_d_fallback(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "conversationId"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "d:intents":
                answer = ac.compute_d_intents(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "intent"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "d:domains":
                answer = ac.compute_d_domains(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "domain"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif metric == "b:response":
                answer = ac.compute_b_response(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "count": answer[0],
                        "items": answer[1],
                        "type": "score"
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            # else:
            #     raise ValueError("`metric` has not a valid value")

            # save analytics and put the _id to retrieve it

            project = analytic['project']
            index_name = "analytic-" + project.lower() + "-" + analytic['dimension']
            query = es.index(index=index_name, doc_type='_doc', body=json_response)

            json_response['id'] = query['_id']
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 200

            return resp
    elif str(analytic['type']).lower() == "aggregation":
        if AggregationComputation.aggregation_validity_check(analytic):
            aggregation = str(analytic['aggregation']).lower()
            json_response = {}
            ac = AggregationComputation()
            if aggregation == "max":
                answer = ac.max_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "max": answer
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif aggregation == "min":
                answer = ac.min_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "min": answer
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif aggregation == "avg":
                answer = ac.avg_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "avg": answer
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif aggregation == "stats":
                answer = ac.stats_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "stats": answer
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif aggregation == "sum":
                answer = ac.sum_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "sum": answer
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif aggregation == "value_count":
                answer = ac.value_count_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "value_count": answer
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif aggregation == "cardinality":
                answer = ac.cardinality_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "cardinality": answer
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif aggregation == "extended_stats":
                answer = ac.extended_stats_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "extended_stats": answer
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            elif aggregation == "percentiles":
                answer = ac.percentiles_aggr(analytic, es, analytic['project'])
                json_response = {
                    "query": analytic,
                    "result": {
                        "percentiles": answer
                    },
                    "status": "ok",
                    "code": 200,
                    "staticId": static_id
                }
            # else:
            #     raise ValueError("`aggregation` has not a valid value")

            # save analytics and put the _id to retrieve it

            project = analytic['project']
            index_name = "analytic-" + project.lower() + "-" + analytic['aggregation']
            query = es.index(index=index_name, doc_type='_doc', body=json_response)

            json_response['id'] = query['_id']
            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 200

            return resp
    else:
        abort(400, message="Invalid value for `type`")


class GetNoClickPerUser(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self):
        if 'userId' in request.args:
            response = self._es.search(index="logging-memex*", body={"query": {"match": {"metadata.userId": request.args['userId']}}})
            if response['hits']['total']['value'] != 0:
                event_collection = {}
                # TODO check da qualche parte su namespace per capire se sto contando un evento
                for item in response['hits']['hits']:
                    if 'metadata' in item['_source']:
                        if 'eventId' in item['_source']['metadata']:
                            if item['_source']['metadata']['eventId'] in event_collection:
                                event_collection[item['_source']['metadata']['eventId']] = event_collection[item['_source']['metadata']['eventId']] + 1
                            else:
                                event_collection[item['_source']['metadata']['eventId']] = 1

                json_response = {
                    "type": "click_per_user",
                    "user": request.args['userId'],
                    "click": event_collection,
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

            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 200

            return resp
        else:
            abort(400, message="Parameter `userId` is needed")


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

            resp = Response(json.dumps(json_response), mimetype='application/json')
            resp.status_code = 200

            return resp
        else:
            abort(400, message="Parameter `eventId` is needed")
