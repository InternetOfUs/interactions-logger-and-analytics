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

import re
import time
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk

from memex_logging.migration.migration import MigrationAction


class RemoveAppIdFromIndicesMigration(MigrationAction):

    def apply(self, es: Elasticsearch) -> None:
        for index in es.indices.get('message-*'):
            # apply operations on indices like this:
            # * message-project-2022-22-11
            # * message-project-history
            # * message-pro-ject-2022-22-11
            # * message-pro-ject-history
            # we don't have to apply them if they are already in the resulting format:
            # * message-2022-22-11
            # * message-history
            splits = index.split("-")
            if len(splits) > 2 and splits[-1] == "history":
                end_of_index = splits[-1]
            elif len(splits) > 4:
                try:
                    index_datetime = datetime.strptime(f"{splits[-3]}-{splits[-2]}-{splits[-1]}", "%Y-%m-%d")
                    end_of_index = index_datetime.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            else:
                continue

            new_index = f"message-{end_of_index}"
            if index != new_index:
                raw_task = es.reindex({
                    "source": {
                        "index": index
                    },
                    "dest": {
                        "index": new_index
                    }
                }, wait_for_completion=False)
                task = es.tasks.get(task_id=raw_task["task"])
                while not task["completed"]:
                    time.sleep(10)
                    task = es.tasks.get(task_id=raw_task["task"])
                es.indices.delete(index)

        for index in es.indices.get('analytic-*'):
            # apply operations on indices like this:
            # * analytic-project-user
            # * analytic-pro-ject-user
            # we don't have to apply them if they are already in the resulting format:
            # * analytic-2022-22-11
            # * analytic-history
            if not re.match(r"^analytic-([0-9]+)-([0-9]+)-([0-9]+)$", index) and not index == "analytic-history":
                analytics = scan(es, index=index, query={"query": {"match_all": {}}})
                actions = []
                for analytic in analytics:
                    creation_dt = datetime.fromisoformat(analytic['_source']['result']['creationDt']) if analytic['_source'].get('result') is not None else datetime.now()
                    new_index = f"analytic-{creation_dt.strftime('%Y-%m-%d')}"
                    if index != new_index:
                        actions.append({**{
                            '_op_type': 'index',
                            '_index': new_index,
                            '_type': analytic['_type']
                        }, **analytic['_source']})
                bulk(es, actions)
                es.indices.delete(index)

    @property
    def action_name(self) -> str:
        return "remove_app_id_from_indices"

    @property
    def action_num(self) -> int:
        return 7
