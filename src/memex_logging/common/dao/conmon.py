# Copyright 2020 U-Hopper srl
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

from datetime import datetime
from typing import Optional

from elasticsearch import Elasticsearch

from memex_logging.utils.utils import Utils


class EntryNotFound(Exception):
    pass


class CommonDao:

    def __init__(self, es: Elasticsearch, base_index: str) -> None:
        self._es = es
        self._base_index = base_index

    def generate_index(self, project: Optional[str] = None, dt: Optional[datetime] = None) -> str:
        return Utils.generate_index(self._base_index, project=project, dt=dt)

    @staticmethod
    def build_query_by_id(trace_id: str) -> dict:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "_id": trace_id
                            }
                        }
                    ]
                }
            }
        }

    @staticmethod
    def build_time_range_query(from_time: datetime, to_time: datetime) -> dict:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": from_time.isoformat(),
                                    "lte": to_time.isoformat()
                                }
                            }
                        }
                    ]
                }
            }
        }
