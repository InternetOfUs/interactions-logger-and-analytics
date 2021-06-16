from memex_logging.ws import celery
from memex_logging.ws.main import build_production_app
from memex_logging.ws.ws import WsInterface


app = build_production_app()
WsInterface.init_celery(celery, app)
