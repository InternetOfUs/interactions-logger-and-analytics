# Copyright 2021 U-Hopper srl
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
import logging
import uuid

from elasticsearch import Elasticsearch
from flask import request, Response
from flask_restful import Resource, abort

from memex_logging.common.analytic.builder import AnalyticBuilder
from memex_logging.celery.analytic import update_analytic
from memex_logging.common.model.aggregation import AggregationAnalytic
from memex_logging.common.model.analytic import DimensionAnalytic
from memex_logging.common.model.response import AnalyticResponse, AggregationResponse
from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.resource.analytic")


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
        logger.info(f"Retrieving analytic with static_id: {static_id}")
        if static_id == "" or static_id is None:
            logger.info("Missing required staticId parameter")
            return {
                "status": "Malformed request: missing required parameter `staticId`",
                "code": 400
            }, 400

        project = request.args.get('project', None)
        index_name = Utils.generate_index("analytic", project=project)
        response = self._es.search(index=index_name, body={"query": {"match": {"staticId.keyword": static_id}}})

        if response['hits']['total']['value'] == 0:
            logger.info("Resource not found")
            return {
                "status": "Not found: resource not found",
                "code": 404
            }, 404
        else:
            return response['hits']['hits'][0]['_source'], 200

    def post(self):
        analytic = request.json
        logger.info(f"Creating analytic: {analytic}")
        if analytic is None:
            logger.info("Analytic failed to be computed due to missing data")
            return {
                "status": "Malformed request: data is missing",
                "code": 400
            }, 400

        try:
            analytic = AnalyticBuilder.from_repr(analytic)
        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logger.info("Error while parsing input analytic data", exc_info=e)
            return {
                "status": f"Malformed request: analytic not valid. Cause: {e.args[0]}",
                "code": 400
            }, 400
        except Exception as e:
            logger.exception("Something went wrong in parsing the analytic", exc_info=e)
            return {
                "status": "Internal server error: something went wrong in parsing the analytic",
                "code": 500
            }, 500

        static_id = str(uuid.uuid4())
        if isinstance(analytic, DimensionAnalytic):
            index_name = "analytic-" + analytic.project.lower() + "-" + analytic.dimension.lower()
            self.es.index(index=index_name, doc_type='_doc', body=AnalyticResponse(analytic, None, static_id).to_repr())
            logger.info("Response stored in " + str(index_name))

        elif isinstance(analytic, AggregationAnalytic):
            index_name = "analytic-" + analytic.project.lower() + "-" + analytic.aggregation.lower()
            self.es.index(index=index_name, doc_type='_doc', body=AggregationResponse(analytic, None, static_id).to_repr())
            logger.info("Response stored in " + str(index_name))

        else:
            logger.error(f"Unrecognized class of analytic [{type(analytic)}]")
            return {
                "status": "Internal server error: something went wrong in handling the analytic",
                "code": 500
            }, 500

        update_analytic.delay(static_id=static_id)
        return {"staticId": static_id}, 200


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
