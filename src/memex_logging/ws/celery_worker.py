from memex_logging.ws import celery
from memex_logging.ws.main import build_interface_from_env


ws_interface = build_interface_from_env()
ws_interface.init_celery(celery)
