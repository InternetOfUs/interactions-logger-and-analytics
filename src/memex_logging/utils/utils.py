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

from datetime import datetime, timezone, timedelta
import logging
import uuid
from typing import Optional, Tuple

import dateutil.parser
from elasticsearch import Elasticsearch
from flask_restful import abort

from memex_logging.common.model.message import RequestMessage, ResponseMessage, NotificationMessage
from memex_logging.common.model.time import DefaultTime, CustomTime

logger = logging.getLogger("logger.utils.utils")


class Utils:

    @staticmethod
    def generate_index(data_type: str, project: Optional[str] = None, dt: Optional[datetime] = None) -> str:
        """
        Generate the Elasticsearch index, the format is `data_type-project-%Y-%m-%d`.

        :param str data_type: the type of data
        :param Optional[str] project: the project associated to the data
        :param Optional[datetime] dt: the datetime of the data
        :return: the generated Elasticsearch index
        :raise ValueError: when there is a datetime but not a project
        """

        if project:
            if dt:
                formatted_date = dt.strftime("%Y-%m-%d")
                index_name = f"{data_type.lower()}-{project.lower()}-{formatted_date}"
            else:
                index_name = f"{data_type.lower()}-{project.lower()}-*"
        else:
            if dt:
                raise ValueError("There is a datetime but not a project")
            else:
                index_name = f"{data_type.lower()}-*"

        return index_name

    @staticmethod
    def extract_range_timestamps(time_object: dict) -> Tuple[str, str]:
        if str(time_object['type']).upper() == DefaultTime.DEFAULT_TIME_TYPE:
            if str(time_object['value']).upper() == "30D":
                now = datetime.now()
                delta = timedelta(days=30)
                temp_old = now - delta
                return temp_old.isoformat(), now.isoformat()
            elif str(time_object['value']).upper() == "10D":
                now = datetime.now()
                delta = timedelta(days=10)
                temp_old = now - delta
                return temp_old.isoformat(), now.isoformat()
            elif str(time_object['value']).upper() == "7D":
                now = datetime.now()
                delta = timedelta(days=7)
                temp_old = now - delta
                return temp_old.isoformat(), now.isoformat()
            elif str(time_object['value']).upper() == "1D":
                now = datetime.now()
                delta = timedelta(days=1)
                temp_old = now - delta
                return temp_old.isoformat(), now.isoformat()
            elif str(time_object['value']).upper() == "TODAY":
                now = datetime.now()
                temp_old = datetime(now.year, now.month, now.day)
                return temp_old.isoformat(), now.isoformat()
            else:
                raise ValueError(f"Unable to handle the interval [{time_object['value']}]")
        elif str(time_object['type']).upper() == CustomTime.CUSTOM_TIME_TYPE:
            start = datetime.fromisoformat(time_object['start']).isoformat()
            end = datetime.fromisoformat(time_object['end']).isoformat()
            return start, end
        else:
            raise ValueError("Unrecognized type for timespan")

    # TODO stop using this and remove!!!!!
    @staticmethod
    def extract_date(data: dict) -> str:
        """
        Extract the date of the message from the message
        """

        if "timestamp" in data.keys():
            try:
                positioned = dateutil.parser.parse(data['timestamp'])
                return str(positioned.year) + "-" + str(positioned.month) + "-" + str(positioned.day)
            except:
                logging.error("`timestamp` of the message cannot be parsed")
                logging.error(data)
        else:
            support_bound = datetime.now().isoformat()
            positioned = dateutil.parser.parse(support_bound)
            return str(positioned.year) + "-" + str(positioned.month) + "-" + str(positioned.day)

    @staticmethod
    def extract_trace_id(data: dict) -> str:
        """
        Extract the trace_id of the message from the message
        """

        if "traceId" in data.keys():
            return data["traceId"]
        else:
            logging.error("`traceId` not found in the message parsed")
            abort(400, message="Invalid message. traceId is missing")

    @staticmethod
    def extract_project_name(data: dict) -> str:
        """
        Extract the name of the project to use the right index on elastic
        """

        if "project" in data.keys():
            return str(data["project"]).lower()
        else:
            return "memex"

    @staticmethod
    def compute_conversation_id(elastic: Elasticsearch, message) -> None:
        # 3 hours threshold
        delta = 90

        if isinstance(message, RequestMessage) or isinstance(message, ResponseMessage) or isinstance(message, NotificationMessage):
            if message.conversation_id is None:
                index = "message-" + str(message.project).lower() + "*"
                body = {
                    "query": {
                        "match": {
                          "userId": message.user_id
                        }
                    },
                    "size": 1,
                    "sort": [
                      {
                        "timestamp": {
                          "order": "desc"
                        }
                      }
                    ]
                }
                response = elastic.search(index=index, body=body, size=1)
                if response['hits']['total'] != 0:
                    positioned = dateutil.parser.parse(response['hits']['hits'][0]['_source']['timestamp'])
                    now = datetime.now().isoformat()
                    positioned_now = dateutil.parser.parse(now)
                    positioned = positioned.replace(tzinfo=timezone.utc).astimezone(tz=None)
                    positioned_now = positioned_now.replace(tzinfo=timezone.utc).astimezone(tz=None)
                    step = positioned_now - positioned
                    if step.seconds > delta:
                        message.conversation_id = uuid.uuid1()
                    else:
                        message.conversation_id = response['hits']['hits'][0]['_source']['conversationId']
