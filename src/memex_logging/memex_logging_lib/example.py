from memex_logging.memex_logging_lib.logging_utils import LoggingUtility, Entity, TextResponse, AttachmentResponse, \
    QuickReplyResponse, CarouselResponse

logger = LoggingUtility("http://127.0.0.1", 80, "memex")

# Log a request message
logger.add_request("Questo è un test", "text", "MSG_001", "USER_001", "CONV_001", "STR_001", "TELEGRAM", "2019-06-14",
                   domain="generic_info", intent_name="test_message", intent_confidence=1.0, language="IT")

# Log a request message and specify some entities
entity_list = []
e = Entity("@city", "Boston", confidence=0.94)
e2 = Entity("@poi", "Pizzeria", confidence=0.83)
entity_list.append(e)
entity_list.append(e2)
logger.add_request("Questo è un altro test", "action", "MSG_003", "USER_002", "CONV_002", "STR_002", "FACEBOOK",
                   "2019-06-14", entities=entity_list)

# Log a text response message
logger.add_response("MSG_004", "CONV_004", "FACEBOOK", "USR_005532", "STR_310", "MSG_001", "2019-06-14")

# Log a notification
content = []
c = AttachmentResponse("https://www.google.com/image.png", "Google's logo")
content.append(c)
logger.add_notification("MSG_00745", "CONV_39210", "STR33151", "TELEGRAM", "USR_319", "2019-06-11", content=content,
                        metadata={'is_available': 'true'})
