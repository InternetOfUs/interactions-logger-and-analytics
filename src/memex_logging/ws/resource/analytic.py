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

import logging
import uuid

from flask import request
from flask_restful import Resource

from memex_logging.celery.analytic import update_analytic, update_analytics
from memex_logging.common.dao.collector import DaoCollector
from memex_logging.common.dao.common import DocumentNotFound
from memex_logging.common.model.analytic.analytic import Analytic
from memex_logging.common.model.analytic.descriptor.builder import AnalyticDescriptorBuilder
from memex_logging.common.model.analytic.time import MovingTimeWindow, FixedTimeWindow, TimeWindow

logger = logging.getLogger("logger.resource.analytic")


class AnalyticsResourceBuilder:
    @staticmethod
    def routes(dao_collector: DaoCollector):
        return [
            (AnalyticInterface, '/analytic', (dao_collector,)),
            (ComputeAnalyticInterface, '/analytic/compute', ()),
            # (GetNoClickPerUser, '/analytic/usercount', (es,)),
            # (GetNoClickPerEvent, '/analytic/eventcount', (es,))
        ]


class AnalyticInterface(Resource):

    def __init__(self, dao_collector: DaoCollector):
        self._dao_collector = dao_collector

    def get(self):
        analytic_id = request.args.get("id")
        logger.info(f"Retrieving analytic with id [{analytic_id}]")
        if analytic_id == "" or analytic_id is None:
            logger.debug("Analytic id has not been specified")
            return {
                "status": "Malformed request: missing required parameter `id`",
                "code": 400
            }, 400

        try:
            analytic = self._dao_collector.analytic.get(analytic_id)
            return analytic.to_repr(), 200
        except DocumentNotFound as e:
            logger.debug(f"Analytic [{analytic_id}] not found", exc_info=e)
            return {
                "status": f"Analytic [{analytic_id}] does not exist",
                "code": 404
            }, 404
        except Exception as e:
            logger.exception(f"Something went wrong while parsing analytic [{analytic_id}]", exc_info=e)
            return {
                "status": f"Something went wrong while parsing analytic [{analytic_id}]",
                "code": 500
            }, 500

    def post(self):
        body = request.json
        logger.info("Defining new analytic")
        if body is None:
            logger.debug("Could not build analytic descriptor: no data was posted")
            return {
                "status": "Malformed request: data is missing",
                "code": 400
            }, 400

        try:
            descriptor = AnalyticDescriptorBuilder.build(body)
        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logger.warning("Error while parsing input analytic data", exc_info=e)
            return {
                "status": f"Malformed request: analytic not valid.",
                "code": 400
            }, 400
        except Exception as e:
            logger.exception("Something went wrong while parsing the provided analytic description", exc_info=e)
            return {
                "status": "Something went wrong while parsing the posted analytic",
                "code": 500
            }, 500

        analytic = Analytic(str(uuid.uuid4()), descriptor, result=None)
        try:
            self._dao_collector.analytic.add(analytic)
            logger.debug(f"Stored new analytic with id [{analytic.analytic_id}]")
        except Exception as e:
            logger.warning(f"Analytic could not be stored [{analytic.to_repr()}]", exc_info=e)
            return {
                "status": "Could not create the analytic",
                "code": 500
            }, 500

        return {
            "id": analytic.analytic_id
        }, 200

    def delete(self):
        analytic_id = request.args.get("id")
        logger.info(f"Deleting analytic with id [{analytic_id}]")
        if analytic_id == "" or analytic_id is None:
            logger.debug("Analytic id has not been specified")
            return {
                "status": "Malformed request: missing required parameter `id`",
                "code": 400
            }, 400

        try:
            self._dao_collector.analytic.delete(analytic_id)
            logger.debug(f"Analytic with id [{analytic_id}] deleted")
        except Exception as e:
            logger.exception(f"Analytic with id [{analytic_id}] could not be to be deleted", exc_info=e)
            return {
                "status": "Could not delete the requested analytic",
                "code": 500
            }, 500

        return {}, 200


class ComputeAnalyticInterface(Resource):

    def post(self):
        analytic_id = request.args.get("id", None)
        time_window_type = request.args.get("timeWindowType", None)
        if analytic_id is None:
            if time_window_type is not None and time_window_type not in TimeWindow.allowed_types():
                logger.debug(f"Unrecognized type [{time_window_type}] for TimeWindow")
                return {
                    "status": "Malformed request: unrecognized value for parameter `timeWindowType`",
                    "code": 400
                }, 400
            
            logger.info(f"Re-computing analytics with window [{time_window_type}] ")
            update_analytics.delay(time_window_type=time_window_type)
        else:
            logger.info(f"Re-computing analytic [{analytic_id}]")
            update_analytic.delay(analytic_id)
        return {}, 200


# class GetNoClickPerUser(Resource):
#
#     def __init__(self, es: Elasticsearch):
#         self._es = es
#
#     def get(self):
#         if 'userId' in request.args:
#             response = self._es.search(index="logging-memex*", body={"query": {"match": {"metadata.userId": request.args['userId']}}})
#             if response['hits']['total']['value'] != 0:
#                 event_collection = {}
#                 # TODO check da qualche parte su namespace per capire se sto contando un evento
#                 for item in response['hits']['hits']:
#                     if 'metadata' in item['_source']:
#                         if 'eventId' in item['_source']['metadata']:
#                             if item['_source']['metadata']['eventId'] in event_collection:
#                                 event_collection[item['_source']['metadata']['eventId']] = event_collection[item['_source']['metadata']['eventId']] + 1
#                             else:
#                                 event_collection[item['_source']['metadata']['eventId']] = 1
#
#                 json_response = {
#                     "type": "click_per_user",
#                     "user": request.args['userId'],
#                     "click": event_collection,
#                     "status": "ok",
#                     "code": 200
#                 }
#             else:
#                 json_response = {
#                     "type": "click_per_user",
#                     "user": request.args['userId'],
#                     "click": {},
#                     "status": "ok",
#                     "code": 200
#                 }
#
#             resp = Response(json.dumps(json_response), mimetype='application/json')
#             resp.status_code = 200
#
#             return resp
#         else:
#             abort(400, message="Parameter `userId` is needed")
#
#
# class GetNoClickPerEvent(Resource):
#
#     def __init__(self, es: Elasticsearch):
#         self._es = es
#
#     def get(self):
#         if 'eventId' in request.args:
#             response = self._es.search(index="logging-memex*", body={"query": {"match": {"metadata.eventId": request.args['eventId']}}})
#             if response['hits']['total']['value'] != 0:
#                 user_collection = {}
#                 for item in response['hits']['hits']:
#                     if 'metadata' in item['_source']:
#                         if 'userId' in item['_source']['metadata']:
#                             if item['_source']['metadata']['userId'] in user_collection:
#                                 user_collection[item['_source']['metadata']['userId']] = user_collection[item['_source']['metadata']['userId']] + 1
#                             else:
#                                 user_collection[item['_source']['metadata']['userId']] = 1
#
#                 json_response = {
#                     "type": "click_per_event",
#                     "event": request.args['eventId'],
#                     "click": user_collection,
#                     "status": "ok",
#                     "code": 200
#                 }
#             else:
#                 json_response = {
#                     "type": "click_per_event",
#                     "event": request.args['eventId'],
#                     "click": {},
#                     "status": "ok",
#                     "code": 200
#                 }
#
#             resp = Response(json.dumps(json_response), mimetype='application/json')
#             resp.status_code = 200
#
#             return resp
#         else:
#             abort(400, message="Parameter `eventId` is needed")
