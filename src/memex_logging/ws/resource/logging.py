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

from elasticsearch import Elasticsearch
from flask import request
from flask_restful import Resource

from memex_logging.common.model.log import Log
from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.resource.logging")


class LoggingResourceBuilder(object):
    """
    Logic class used to create enable the endpoints. This class is used in ws.py
    """
    @staticmethod
    def routes(es: Elasticsearch):
        return [
            (LogGeneralLog, '/log', (es,)),
            (LogGeneralLogs, '/logs', (es,))
        ]


class LogGeneralLog(Resource):
    """
    This class can be used to log a single log message into the elasticsearch instance
    """

    def __init__(self, es: Elasticsearch):
        """
        Function to gather the parameters that are going to be used in the various methods
        :param es: the elastic search instance
        """
        self._es = es

    # TODO get, update and delete log


class LogGeneralLogs(Resource):
    """
    This class can be used to log an array of log messages.
    """

    def __init__(self, es: Elasticsearch):
        """
        Function to gather the external parameters that are going to be used in the other methods
        :param es: the elasticsearch instance
        """
        self._es = es

    def post(self):
        """
        Add a batch of log messages to the database. The logs must be passed in the request body.
        This method log the messages with index <project-name>-logging-<yyyy>-<mm>-<dd>
        :return: the HTTP response
        """
        logger.info("Starting to log a new set of messages")

        logs_received = request.json
        log_ids = []

        for log in logs_received:
            # store the message in the database
            try:
                temp_log = Log.from_repr(log)
                project_name = Utils.extract_project_name(log)
                date = Utils.extract_date(log)
                index_name = "logging-" + project_name + "-" + date
                query = self._es.index(index=index_name, doc_type='_doc', body=temp_log.to_repr())
                log_ids.append(query['_id'])
            except Exception as e:
                logger.error(f"Failed to log: {log}", exc_info=e)

        return {
            "traceIds": log_ids,
            "status": "Created: logs stored",
            "code": 201
        }, 201
