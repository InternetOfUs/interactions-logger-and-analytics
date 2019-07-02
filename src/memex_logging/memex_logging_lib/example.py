from memex_logging.memex_logging_lib.logging_utils import LoggingUtility

import datetime

import json

logger = LoggingUtility("http://127.0.0.1", "max_test_2")

# Log a request message
#print(logger.add_textual_request("Questa è una prova per action", "MSG003", "USR001", "MESSENGER", datetime.datetime.now().isoformat(), metadata={'a': 'b'}))
#print(logger.add_action_request("Action test", "MSG004", "USR002", "TELEGRAM", datetime.datetime.now().isoformat(), conversation_id="CONV002", domain="test", language="IT"))

#print(logger.add_attachment_request("http://www.google.com/photo.png", "MSG006", "USR002", "MESSENGER", datetime.datetime.now().isoformat(), alternative_text="I am an image", domain="boh", language="EN"))

#print(logger.add_location_request(47.043,52.3134,"MSG003", "USR001", "MESSENGER", datetime.datetime.now().isoformat()))

#print(logger.add_textual_response("My first response", "MSG_RES_005", "ALEXA", "USR_009", "MSG_002", int(datetime.datetime.now().timestamp()), conversation_id="CONV_002", metadata={"a": "b"}, buttons=[("ciao","BTN001"),("hi", "BTN002")]))

#print(logger.add_attachment_response("http://www.facebook.com/logo.png", "MSG_1022", "Facebook", "USR002", "MSG001", int(datetime.datetime.now().timestamp()),buttons=[("Come stai?", None),("How are you?", None)]))

#logger.get_message_from_message_id("MSG003")

logger.get_message_from_generic_id('leGzr2sBrC2LJx_SXs7i')
