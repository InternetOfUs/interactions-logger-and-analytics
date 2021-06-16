import os

from celery import Celery


def make_celery(app_name=__name__):
    return Celery(
            app_name,
            backend=os.getenv("CELERY_RESULT_BACKEND", None),
            broker=os.getenv("CELERY_BROKER_URL", None)
        )


celery = make_celery()
