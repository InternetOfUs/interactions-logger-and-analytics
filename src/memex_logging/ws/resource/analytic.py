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

from elasticsearch import Elasticsearch
from flask import request
from flask_restful import Resource

from memex_logging.celery.analytic import update_moving_time_window_analytics, update_analytic, \
    update_fixed_time_window_analytics, update_all_analytics
from memex_logging.common.model.analytic.analytic import Analytic
from memex_logging.common.model.analytic.descriptor.aggregation import AggregationDescriptor
from memex_logging.common.model.analytic.descriptor.builder import AnalyticDescriptorBuilder
from memex_logging.common.model.analytic.descriptor.count import CountDescriptor
from memex_logging.common.model.analytic.descriptor.segmentation import SegmentationDescriptor
from memex_logging.common.model.analytic.time import MovingTimeWindow, FixedTimeWindow
from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.resource.analytic")


class AnalyticsResourceBuilder:
    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (AnalyticInterface, '/analytic', (es,)),
            (ComputeAnalyticInterface, '/analytic/compute', ()),
            # (GetNoClickPerUser, '/analytic/usercount', (es,)),
            # (GetNoClickPerEvent, '/analytic/eventcount', (es,))
        ]


class AnalyticInterface(Resource):

    def __init__(self, es: Elasticsearch):
        self._es = es

    def get(self):
        analytic_id = request.args.get("id")
        logger.info(f"Retrieving analytic with id [{analytic_id}]")
        if analytic_id == "" or analytic_id is None:
            logger.debug("Analytic id has not been specified")
            return {
                "status": "Malformed request: missing required parameter `id`",
                "code": 400
            }, 400

        project = request.args.get("project", None)
        index_name = Utils.generate_index("analytic", project=project)
        try:
            # TODO should be moved into a dedicated dao
            response = self._es.search(index=index_name, body={"query": {"match": {"id.keyword": analytic_id}}})
        except Exception as e:
            logger.exception(f"Analytic with id [{analytic_id}] could not be retrieved", exc_info=e)
            return {
                "status": "Internal server error: could not retrieved the analytic",
                "code": 500
            }, 500

        if response['hits']['total']['value'] == 0:
            logger.debug(f"Analytic with id [{analytic_id}] not found")
            return {
                "status": f"Not found: analytic with id [{analytic_id}] not found",
                "code": 404
            }, 404
        else:
            logger.debug(f"Analytic with id [{analytic_id}] retrieved")
            analytic = Analytic.from_repr(response['hits']['hits'][0]['_source'])
            return analytic.to_repr(), 200

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
                "status": f"Malformed request: analytic not valid. Cause: {e.args[0]}",
                "code": 400
            }, 400
        except Exception as e:
            logger.exception("Something went wrong in parsing the analytic", exc_info=e)
            return {
                "status": "Internal server error: something went wrong while parsing the posted analytic",
                "code": 500
            }, 500

        # TODO should we maybe using the Utils.generate_index() method?
        if isinstance(descriptor, CountDescriptor):
            index_name = "analytic-" + descriptor.project.lower() + "-" + descriptor.dimension.lower()  # TODO it should always be lowercase at this stage
        elif isinstance(descriptor, AggregationDescriptor):
            index_name = "analytic-" + descriptor.project.lower() + "-" + descriptor.aggregation.lower()  # TODO it should always be lowercase at this stage
        elif isinstance(descriptor, SegmentationDescriptor):
            index_name = "analytic-" + descriptor.project.lower() + "-" + descriptor.dimension.lower()  # TODO it should always be lowercase at this stage
        else:
            logger.error(f"Un-supported analytic descriptor of type [{type(descriptor)}]")
            return {
                "status": "Internal server error: something went wrong while building the new analytic",
                "code": 500
            }, 500

        new_analytic_id = str(uuid.uuid4())
        analytic = Analytic(new_analytic_id, descriptor, result=None)

        try:
            # TODO should be moved into a dedicated dao
            self._es.index(index=index_name, doc_type='_doc', body=analytic.to_repr())
            logger.debug(f"Analytic stored in index [{index_name}] with id [{analytic.analytic_id}]")
        except Exception as e:
            logger.exception(f"Analytic could not be stored [{analytic.to_repr()}]", exc_info=e)
            return {
                "status": "Internal server error: could not create the analytic",
                "code": 500
            }, 500

        return {
            "id": new_analytic_id
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

        project = request.args.get("project", None)
        index_name = Utils.generate_index("analytic", project=project)
        try:
            # TODO should be moved into a dedicated dao
            self._es.delete_by_query(index=index_name, body={"query": {"match": {"id.keyword": analytic_id}}})
            logger.debug(f"Analytic with id [{analytic_id}] deleted")
        except Exception as e:
            logger.exception(f"Analytic with id [{analytic_id}] could not be to be deleted", exc_info=e)
            return {
                "status": "Internal server error: could not delete the requested analytic",
                "code": 500
            }, 500

        return {}, 200


class ComputeAnalyticInterface(Resource):

    def post(self):
        analytic_id = request.args.get("id", None)
        time_window_type = request.args.get("type", None)
        if analytic_id is None:
            if time_window_type is None:
                logger.info("Updating all analytics")
                update_all_analytics.delay()
            elif time_window_type == MovingTimeWindow.type() or time_window_type == MovingTimeWindow.deprecated_type():
                logger.info("Updating moving time window analytics")
                update_moving_time_window_analytics.delay()
            elif time_window_type == FixedTimeWindow.type() or time_window_type == FixedTimeWindow.deprecated_type():
                logger.info("Updating fixed time window analytics")
                update_fixed_time_window_analytics.delay()
            else:
                logger.info(f"Unrecognized type [{time_window_type}] for TimeWindow")
                return {
                           "status": "Malformed request: unrecognized value for parameter `type`",
                           "code": 400
                       }, 400
        else:
            logger.info(f"Updating analytic with id {analytic_id}")
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
