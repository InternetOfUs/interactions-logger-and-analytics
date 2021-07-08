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

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from memex_logging.common.utils import Utils
from memex_logging.migration.migration import MigrationAction


class MovingTimeWindowMigration(MigrationAction):

    def apply(self, es: Elasticsearch) -> None:
        index_name = Utils.generate_index("analytic")
        results = scan(es, index=index_name, query={"query": {"match": {"query.timespan.type.keyword": "DEFAULT"}}})

        for result in results:
            result['_source']["query"]["timespan"]["type"] = "MOVING"
            es.index(index=result['_index'], id=result['_id'], doc_type=result['_type'], body=result['_source'])

    @property
    def action_name(self) -> str:
        return "moving_time_window_type_renamed"

    @property
    def action_num(self) -> int:
        return 1
