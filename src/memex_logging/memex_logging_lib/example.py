from memex_logging.memex_logging_lib.logging_utils import LoggingUtility

logger = LoggingUtility("http://127.0.0.1", 80, "memex")

messages = [
    {
        "messageId": "2A6F67A4-42D2-4DE0-9F00-DE4A564A83A8",
        "channel": "MESSENGER",
        "userId": "USR-JDKHEIU2-31NJDTE94",
        "conversationId": "CONV-DAJKWPENVOA-314CAD",
        "structureId": "STR-FIWN25S-DAJO94",
        "timestamp": "2019-04-04:23.11.58",
        "content": {
            "type": "text",
            "value": "Voglio fare un bonifico"
        },
        "domain": "pagamenti",
        "intent": {
            "name": "bonifico",
            "confidence": 0.87
        },
        "entities": [
            {
                "type": "city",
                "value": "Boston",
                "confidence": 0.94
            },
            {
                "type": "city",
                "value": "London",
                "confidence": 0.81
            },
            {
                "type": "city",
                "value": "Milan",
                "confidence": 0.95
            }
        ],
        "project": "memex",
        "language": "IT",
        "metadata": {},
        "type": "request"
    },
    {
            "messageId": "2A6F67A4-42D2-4DE0-9F00-DE4A564A83A8",
            "channel": "MESSENGER",
            "userId": "USR-JDKHEIU2-31NJDTE94",
            "conversationId": "CONV-DAJKWPENVOA-314CAD",
            "structureId": "STR-FIWN25S-DAJO94",
            "timestamp": "2019-04-04:23.11.58",
            "content": {
                "type": "text",
                "value": "Voglio fare un bonifico"
            },
            "domain": "pagamenti",
            "intent": {
                "name": "bonifico",
                "confidence": 0.87
            },
            "entities": [
                {
                    "type": "city",
                    "value": "Boston",
                    "confidence": 0.94
                },
                {
                    "type": "city",
                    "value": "London",
                    "confidence": 0.81
                },
                {
                    "type": "city",
                    "value": "Milan",
                    "confidence": 0.95
                }
            ],
            "project": "memex",
            "language": "IT",
            "metadata": {},
            "type": "request"
    }
]

# add a message
#logger.add_messages(messages)
# get a message
#logger.get_message("2A6F67A4-42D2-4DE0-9F00-DE4A564A83A8")
# delete a message
logger.delete_message("2A6F67A4-42D2-4DE0-9F00-DE4A564A83A8")