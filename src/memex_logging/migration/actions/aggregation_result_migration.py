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

from copy import deepcopy

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from memex_logging.migration.migration import MigrationAction


class AggregationResultMigration(MigrationAction):

    def apply(self, es: Elasticsearch) -> None:
        index_name = "analytic-*"
        analytics = scan(es, index=index_name, query={"query": {"match": {"result.type.keyword": "aggregation"}}})

        for analytic in analytics:
            stored_analytic = deepcopy(analytic)
            if analytic['_source']['descriptor']['aggregation'] == "avg":
                analytic['_source']['result']['aggregation'] = {"avg": analytic['_source']['result']['aggregation']}
            if analytic['_source']['descriptor']['aggregation'] == "min":
                analytic['_source']['result']['aggregation'] = {"min": analytic['_source']['result']['aggregation']}
            if analytic['_source']['descriptor']['aggregation'] == "max":
                analytic['_source']['result']['aggregation'] = {"max": analytic['_source']['result']['aggregation']}
            if analytic['_source']['descriptor']['aggregation'] == "sum":
                analytic['_source']['result']['aggregation'] = {"sum": analytic['_source']['result']['aggregation']}
            if analytic['_source']['descriptor']['aggregation'] == "value_count":
                analytic['_source']['result']['aggregation'] = {"value_count": analytic['_source']['result']['aggregation']}
            if analytic['_source']['descriptor']['aggregation'] == "cardinality":
                analytic['_source']['result']['aggregation'] = {"cardinality": analytic['_source']['result']['aggregation']}

            if analytic != stored_analytic:
                es.index(index=analytic['_index'], id=analytic['_id'], doc_type=analytic['_type'], body=analytic['_source'])

    @property
    def action_name(self) -> str:
        return "aggregation_result"

    @property
    def action_num(self) -> int:
        return 8
