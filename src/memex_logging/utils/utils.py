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

from flask_restful import abort

import logging
import datetime
from datetime import timezone
import uuid

import dateutil.parser

from elasticsearch import Elasticsearch

from memex_logging.models.message import RequestMessage, ResponseMessage, NotificationMessage


class Utils:

    @staticmethod
    def extract_date(data:dict) -> str:
        """
        :param data:
        :return:
        """
        date = ""
        if "timestamp" in data.keys():
            try:
                positioned = dateutil.parser.parse(data['timestamp'])
                return str(positioned.year) + "-" + str(positioned.month) + "-" + str(positioned.day)
            except:
                logging.error("timestamp cannot be parsed of the message cannot be parsed")
                logging.error(data)
        else:
            support_bound = datetime.datetime.now().isoformat()
            positioned = dateutil.parser.parse(support_bound)
            return str(positioned.year) + "-" + str(positioned.month) + "-" + str(positioned.day)

    @staticmethod
    def extract_trace_id(data: dict) -> str:
        """
        Extract the id of the message from the message
        :param data:
        :return:
        """
        if "traceId" in data.keys():
            return data["traceId"]
        else:
            logging.error("ERROR@Utils - traceId not found in the message parsed")
            abort(400, message="Invalid message. traceId is missing")

    @staticmethod
    def extract_project_name(data: dict) -> str:
        """
        Extract the name of the project to use the right index on elastic
        :param data:
        :return:
        """
        if "project" in data.keys():
            return str(data["project"]).lower()
        else:
            return "memex"

    def compute_conversation_id(self, elastic: Elasticsearch, message) -> str:
        # 3 hours threshold
        delta = 90

        if isinstance(message, RequestMessage) or isinstance(message, ResponseMessage) or isinstance(message, NotificationMessage):
            if message.conversation_id is None:
                index = "message-" + str(message.project).lower() + "*"
                body = {
                    "query": {
                        "match": {
                          "userId" : message.user_id
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
                    now = datetime.datetime.now().isoformat()
                    positioned_now = dateutil.parser.parse(now)
                    positioned = positioned.replace(tzinfo=timezone.utc).astimezone(tz=None)
                    positioned_now = positioned_now.replace(tzinfo=timezone.utc).astimezone(tz=None)
                    step = positioned_now - positioned
                    if step.seconds > delta:
                        message.conversation_id = uuid.uuid1()
                    else:
                        message.conversation_id = response['hits']['hits'][0]['_source']['conversationId']
