from memex_logging.memex_logging_lib.logging_utils import LoggingUtility

logger = LoggingUtility("http://127.0.0.1", 80, "memex")

logger.add_request("Questo è un test","text","MSG_001","USER_001","CONV_001","STR_001","TELEGRAM", "2019-06-14", "memex")