from flask_restful import abort

import logging


class Utils:

    def _compute_conversation_id(self) -> str:
        logging.debug("INFO@Utils - starting to compute the conversation id")
        # TODO compute the convId
        
        return "ABCD"

    def _extract_trace_id(self, data: dict) -> str:
        '''
        Extract the id of the message from the message
        :param data:
        :return:
        '''
        if "traceId" in data.keys():
            return data["traceId"]
        else:
            logging.error("ERROR@Utils - traceId not found in the message parsed")
            abort(400, message="Invalid message. traceId is missing")

    def _time_based_segmentation(self) -> str:
        return ""

    def _intent_based_segmentation(self) -> str:
        return ""