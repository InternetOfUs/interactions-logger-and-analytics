from flask_restful import abort

import logging
import datetime

import dateutil.parser

from elasticsearch import Elasticsearch


class Utils:

    @staticmethod
    def compute_conversation_id() -> str:
        logging.debug("INFO@Utils - starting to compute the conversation id")
        return None

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
            positioned = datetime.datetime.now().isoformat()
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

    @staticmethod
    def get_mapping(elastic: Elasticsearch, project: str, field: str = None) -> dict:
        index = project + "-message*"

        #response = elastic.(index=index)
        response = elastic.indices.get_mapping(index="memex-message*")

        print(response)