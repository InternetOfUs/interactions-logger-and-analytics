from flask_restful import abort

import logging


class Utils:

    @staticmethod
    def compute_conversation_id() -> str:
        logging.debug("INFO@Utils - starting to compute the conversation id")
        return "ABCD"

    def extract_trace_id(self, data: dict) -> str:
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

    def extract_project_name(self, data: dict) -> str:
        """
        Extract the name of the project to use the right index on elastic
        :param data:
        :return:
        """
        if "project" in data.keys():
            return str(data["project"]).lower()
        else:
            return "memex"

    def _time_based_segmentation(self) -> str:
        return ""

    def _intent_based_segmentation(self) -> str:
        return ""