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


class MessageConversationDeletionMigration(MigrationAction):

    def apply(self, es: Elasticsearch) -> None:
        index_name = Utils.generate_index("analytic")
        analytics = scan(es, index=index_name, query={"query": {"match_all": {}}})

        for analytic in analytics:
            # Remove analytics with metric value:
            # - m:conversation
            if analytic['_source']['descriptor'].get('metric') == "m:conversation":
                es.delete(index=analytic['_index'], id=analytic['_id'], doc_type=analytic['_type'])
                continue

    @property
    def action_name(self) -> str:
        return "message_conversation_deletion"

    @property
    def action_num(self) -> int:
        return 6
