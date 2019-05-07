from memex_logging.memex_logging_lib.logging_utils import LoggingUtility

logger = LoggingUtility

message = {
    'traceId': 'logFromApi',
    'message': 'log from class Logging Utils'
}

message2 = {
    'traceId': 'logFromAPI',
    'message': 'second message from class Logging Utils'
}

messages = [message, message2]

logger.add_message(message)
