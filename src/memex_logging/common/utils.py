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

from datetime import datetime, timezone
import logging
import uuid
from typing import Optional, Tuple

import dateutil.parser
from dateutil.relativedelta import relativedelta
from elasticsearch import Elasticsearch
from flask_restful import abort

from memex_logging.common.model.message import RequestMessage, ResponseMessage, NotificationMessage
from memex_logging.common.model.analytic.time import TimeWindow, MovingTimeWindow, FixedTimeWindow


logger = logging.getLogger("logger.utils.utils")


class Utils:

    @staticmethod
    def generate_index(data_type: str, dt: Optional[datetime] = None) -> str:
        """
        Generate the Elasticsearch index, the format is `data_type-%Y-%m-%d`.

        :param str data_type: the type of data
        :param Optional[datetime] dt: the datetime of the data
        :return: the generated Elasticsearch index
        :raise ValueError: when there is a datetime but not a project
        """

        if dt:
            formatted_date = dt.strftime("%Y-%m-%d")
            index_name = f"{data_type.lower()}-{formatted_date}"
        else:
            index_name = f"{data_type.lower()}-*"

        return index_name

    @staticmethod
    def extract_range_timestamps(time_window: TimeWindow) -> Tuple[Optional[datetime], datetime]:
        if isinstance(time_window, MovingTimeWindow):
            now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

            if time_window.descriptor.upper() == "D":
                return now - relativedelta(days=time_window.value), now
            elif time_window.descriptor.upper() == "W":
                return now - relativedelta(weeks=time_window.value), now
            elif time_window.descriptor.upper() == "M":
                return now - relativedelta(months=time_window.value), now
            elif time_window.descriptor.upper() == "Y":
                return now - relativedelta(years=time_window.value), now
            elif time_window.descriptor.upper() == "TODAY":
                return now, now + relativedelta(days=1)
            elif time_window.descriptor.upper() == "ALL":
                return None, now
            else:
                logger.info(f"Unable to handle the value [{time_window.value}{time_window.descriptor}]")
                raise ValueError(f"Unable to handle the value [{time_window.value}{time_window.descriptor}]")
        elif isinstance(time_window, FixedTimeWindow):
            return time_window.start, time_window.end
        else:
            logger.info(f"Unrecognized type [{type(time_window)}] for timespan")
            raise ValueError(f"Unrecognized type [{type(time_window)}] for timespan")

    @staticmethod
    def compute_age(date_of_birth: datetime) -> int:
        now = datetime.now()
        years = now.year - date_of_birth.year
        if now.month >= date_of_birth.month and now.day >= date_of_birth.day:
            age = years
        else:
            age = years - 1
        return age

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
            except Exception as e:
                logging.error("`timestamp` of the message cannot be parsed", exc_info=e)
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
            return data["project"].lower()
        else:
            return "memex"

    @staticmethod
    def compute_conversation_id(elastic: Elasticsearch, message) -> None:
        # 3 hours threshold
        delta = 90

        if isinstance(message, RequestMessage) or isinstance(message, ResponseMessage) or isinstance(message, NotificationMessage):
            if message.conversation_id is None:
                index = "message-" + message.project.lower() + "*"
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
