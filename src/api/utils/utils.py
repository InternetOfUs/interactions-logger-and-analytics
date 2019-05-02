from flask_restful import abort

import logging


class Utils:

    def _compute_conversation_id(self) -> str:
        logging.debug("INFO@Utils - starting to compute the conversation id")
        # TODO compute the convId
        
        return "ABCD"

    def _extract_trace_id(self, data: dict) -> str:
        if "traceId" in data.keys():
            return data["traceId"]
        else:
            logging.error("ERROR@Utils - traceId not found in the message parsed")
            abort(400, message="Invalid message. traceId is missing")