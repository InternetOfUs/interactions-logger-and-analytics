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

import os

from celery import Celery


def make_celery(app_name=__name__):
    return Celery(
            app_name,
            backend=os.getenv("CELERY_RESULT_BACKEND", None),
            broker=os.getenv("CELERY_BROKER_URL", None)
        )


celery = make_celery()
