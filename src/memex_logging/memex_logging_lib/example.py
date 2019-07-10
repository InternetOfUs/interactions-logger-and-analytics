from memex_logging.memex_logging_lib.logging_utils import LoggingUtility, CarouselItem

import datetime

import json

logger = LoggingUtility("http://127.0.0.1", "pd")

# Log a request message
#print(logger.add_textual_request("Questa è una prova per action", "MSG003", "USR001", "MESSENGER", datetime.datetime.now().isoformat()))
#print(logger.add_action_request("Action test", "MSG004", "USR002", "TELEGRAM", datetime.datetime.now().isoformat(), conversation_id="CONV002", domain="test", language="IT"))

#print(logger.add_attachment_request("http://www.google.com/photo.png", "MSG006", "USR002", "MESSENGER", datetime.datetime.now().isoformat(), alternative_text="I am an image", domain="boh", language="EN"))

#print(logger.add_location_request(47.043,52.3134,"MSG003", "USR001", "MESSENGER", datetime.datetime.now().isoformat()))

#print(logger.add_textual_response("My first response", "MSG_RES_005", "ALEXA", "USR_009", "MSG_002", int(datetime.datetime.now().timestamp()), conversation_id="CONV_002", metadata={"a": "b"}, buttons=[("ciao","BTN001"),("hi", "BTN002")]))

#print(logger.add_attachment_response("http://www.facebook.com/logo.png", "MSG_1022", "Facebook", "USR002", "MSG001", int(datetime.datetime.now().timestamp()),buttons=[("Come stai?", None),("How are you?", None)]))

#print(logger.add_location_request(47.16542,13.31415,"MSGE321031231","USR31314","facebook",datetime.datetime.now().isoformat()))

#print(logger.add_textual_response("Prova di risposta","MSG313233213", "facebook", "USR3123141", "MSG003", datetime.datetime.now().isoformat()))

#print(logger.add_attachment_response("http://www.google.com/logo.png","MSG3123123","facebook", "USR31332", "MSG0032", datetime.datetime.now().isoformat()))

#print(logger.add_quick_reply_response([("ciao","BTN31"),("text", "BTN11231")],"MSG312313","facebook","USR3132", "MSG312313", datetime.datetime.now().isoformat()))

item = CarouselItem("Item di prova", "https://www.google.com/logo.png", subtitle="abcde")
item2 = CarouselItem("Nuovo item", None, subtitle="efasasd")

#print(logger.add_carousel_response([item, item2],"MSG31232","telegram","USR312313", "RSP3123", datetime.datetime.now().isoformat()))

print(logger.add_log("LOG3213321","nlu","test","questa è una prova della libreria", datetime.datetime.now().isoformat()))