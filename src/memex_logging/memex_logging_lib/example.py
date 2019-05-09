from memex_logging.memex_logging_lib.logging_utils import LoggingUtility

logger = LoggingUtility("http://127.0.0.1", 80)

message = {
    'traceId': 'logFromApi',
    'message': 'log from class Logging Utils'
}

message2 = {
    'traceId': 'logFromAPI',
    'message': 'second message from class Logging Utils'
}

messages = [message, message2]

# add a single message
logger.add_message(message)
# add a list of messages
logger.add_messages(messages)
# add a single log
logger.add_log(message)
# add a list of logs
logger.add_logs(messages)
# retrieve a message given the id
logger.get_message("logFromApi")
# retrieve a conversation given the id
print(logger.get_conversation("ABCD"))
