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

from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from memex_logging.migration.migration import MigrationAction


class RemoveAppIdFromIndicesMigration(MigrationAction):

    def apply(self, es: Elasticsearch) -> None:
        for index in es.indices.get('message-*'):
            splits = index.split("-")
            try:
                index_datetime = datetime.strptime(f"{splits[-3]}-{splits[-2]}-{splits[-1]}", "%Y-%m-%d")
                end_of_index = index_datetime.strftime("%Y-%m-%d")
            except ValueError:
                end_of_index = splits[-1]

            es.reindex({
                "source": {
                    "index": index
                },
                "dest": {
                    "index": f"message-{end_of_index}"
                }
            })
            es.indices.delete(index)

        for index in es.indices.get('analytic-*'):
            analytics = scan(es, index=index, query={"query": {"match_all": {}}})
            for analytic in analytics:
                creation_dt = datetime.fromisoformat(analytic['_source']['result']['creationDt']) if analytic['_source'].get('result') is not None else datetime.now()
                new_index = f"analytic-{creation_dt.strftime('%Y-%m-%d')}"
                es.index(index=new_index, doc_type=analytic['_type'], body=analytic['_source'])
            es.indices.delete(index)

    @property
    def action_name(self) -> str:
        return "remove_app_id_from_indices"

    @property
    def action_num(self) -> int:
        return 6
