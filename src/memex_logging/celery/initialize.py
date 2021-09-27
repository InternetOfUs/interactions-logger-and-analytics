# Copyright 2021 U-Hopper srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, annotations

from celery import Celery
from celery.schedules import crontab

from memex_logging.celery import celery
from memex_logging.celery.analytic import update_analytics, update_not_concluded_fixed_time_window_analytics
from memex_logging.common.model.analytic.time import MovingTimeWindow
from memex_logging.ws.main import build_interface_from_env


ws_interface = build_interface_from_env()
ws_interface.init_celery(celery)
celery.conf.timezone = 'Europe/Rome'


@celery.on_after_configure.connect
def setup_periodic_tasks(sender: Celery, **kwargs):
    sender.add_periodic_task(crontab(minute=0, hour=4), update_analytics.s(time_window_type=MovingTimeWindow.type()))
    sender.add_periodic_task(crontab(minute=0, hour=4), update_not_concluded_fixed_time_window_analytics.s())


setup_periodic_tasks(celery)
