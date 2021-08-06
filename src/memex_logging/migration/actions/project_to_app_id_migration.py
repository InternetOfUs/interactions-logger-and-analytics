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

from memex_logging.common.utils import Utils
from memex_logging.migration.migration import MigrationAction


class ProjectToAppIdMigration(MigrationAction):

    def apply(self, es: Elasticsearch) -> None:
        # Project to AppId for messages
        index_name = Utils.generate_index("message", project="wenet-ask-for-help-aalborg")
        messages = scan(es, index=index_name, query={"query": {"match": {"project.keyword": "wenet-ask-for-help-aalborg"}}})
        for message in messages:
            message['_source']['project'] = "cG37pczAJx" if datetime.fromisoformat(message['_source']['timestamp']) < datetime(2021, 3, 12) else "2kUw54aeVP"
            index = message['_index']
            es.delete(index=index, id=message['_id'], doc_type=message['_type'])
            index = index.replace("wenet-ask-for-help-aalborg", "cg37pczajx" if datetime.fromisoformat(message['_source']['timestamp']) < datetime(2021, 3, 12) else "2kuw54aevp")
            es.index(index=index, id=message['_id'], doc_type=message['_type'], body=message['_source'])

        index_name = Utils.generate_index("message", project="wenet-ask-for-help-london")
        messages = scan(es, index=index_name, query={"query": {"match": {"project.keyword": "wenet-ask-for-help-london"}}})
        for message in messages:
            message['_source']['project'] = "9tF0K1T7Rr"
            index = message['_index']
            es.delete(index=index, id=message['_id'], doc_type=message['_type'])
            index = index.replace("wenet-ask-for-help-london", "9tf0k1t7rr")
            es.index(index=index, id=message['_id'], doc_type=message['_type'], body=message['_source'])

        index_name = Utils.generate_index("message", project="wenet-ask-for-help-mongolia")
        messages = scan(es, index=index_name, query={"query": {"match": {"project.keyword": "wenet-ask-for-help-mongolia"}}})
        for message in messages:
            message['_source']['project'] = "GnYi1gZEcv"
            index = message['_index']
            es.delete(index=index, id=message['_id'], doc_type=message['_type'])
            index = index.replace("wenet-ask-for-help-mongolia", "gnyi1gzecv")
            es.index(index=index, id=message['_id'], doc_type=message['_type'], body=message['_source'])

        index_name = Utils.generate_index("message", project="wenet-ask-for-help-paraguay")
        messages = scan(es, index=index_name, query={"query": {"match": {"project.keyword": "wenet-ask-for-help-paraguay"}}})
        for message in messages:
            message['_source']['project'] = "jFLFXPUDz4"
            index = message['_index']
            es.delete(index=index, id=message['_id'], doc_type=message['_type'])
            index = index.replace("wenet-ask-for-help-paraguay", "jflfxpudz4")
            es.index(index=index, id=message['_id'], doc_type=message['_type'], body=message['_source'])

        index_name = Utils.generate_index("message", project="wenet-ask-for-help-trento")
        messages = scan(es, index=index_name, query={"query": {"match": {"project.keyword": "wenet-ask-for-help-trento"}}})
        for message in messages:
            message['_source']['project'] = "dLAIbwQczK"
            index = message['_index']
            es.delete(index=index, id=message['_id'], doc_type=message['_type'])
            index = index.replace("wenet-ask-for-help-trento", "dlaibwqczk")
            es.index(index=index, id=message['_id'], doc_type=message['_type'], body=message['_source'])

        index_name = Utils.generate_index("message", project="wenet-ask-for-help")
        messages = scan(es, index=index_name, query={"query": {"match": {"project.keyword": "wenet-ask-for-help"}}})
        for message in messages:
            message['_source']['project'] = "xAcauSmrhd"
            index = message['_index']
            es.delete(index=index, id=message['_id'], doc_type=message['_type'])
            index = index.replace("wenet-ask-for-help", "xacausmrhd")
            es.index(index=index, id=message['_id'], doc_type=message['_type'], body=message['_source'])

        # Project to AppId for analytics
        index_name = Utils.generate_index("analytic", project="wenet-ask-for-help-aalborg")
        analytics = scan(es, index=index_name, query={"query": {"match": {"descriptor.project.keyword": "wenet-ask-for-help-aalborg"}}})
        for analytic in analytics:
            analytic['_source']['descriptor']['project'] = "cG37pczAJx" if datetime.fromisoformat(analytic['_source']["result"]['creationDt']) < datetime(2021, 3, 12) else "2kUw54aeVP"
            index = analytic['_index']
            es.delete(index=index, id=analytic['_id'], doc_type=analytic['_type'])
            index = index.replace("wenet-ask-for-help-aalborg", "cg37pczajx" if datetime.fromisoformat(analytic['_source']["result"]['creationDt']) < datetime(2021, 3, 12) else "2kuw54aevp")
            es.index(index=index, id=analytic['_id'], doc_type=analytic['_type'], body=analytic['_source'])

        index_name = Utils.generate_index("analytic", project="wenet-ask-for-help-london")
        analytics = scan(es, index=index_name, query={"query": {"match": {"descriptor.project.keyword": "wenet-ask-for-help-london"}}})
        for analytic in analytics:
            analytic['_source']['descriptor']['project'] = "9tF0K1T7Rr"
            index = analytic['_index']
            es.delete(index=index, id=analytic['_id'], doc_type=analytic['_type'])
            index = index.replace("wenet-ask-for-help-london", "9tf0k1t7rr")
            es.index(index=index, id=analytic['_id'], doc_type=analytic['_type'], body=analytic['_source'])

        index_name = Utils.generate_index("analytic", project="wenet-ask-for-help-mongolia")
        analytics = scan(es, index=index_name, query={"query": {"match": {"descriptor.project.keyword": "wenet-ask-for-help-mongolia"}}})
        for analytic in analytics:
            analytic['_source']['descriptor']['project'] = "GnYi1gZEcv"
            index = analytic['_index']
            es.delete(index=index, id=analytic['_id'], doc_type=analytic['_type'])
            index = index.replace("wenet-ask-for-help-mongolia", "gnyi1gzecv")
            es.index(index=index, id=analytic['_id'], doc_type=analytic['_type'], body=analytic['_source'])

        index_name = Utils.generate_index("analytic", project="wenet-ask-for-help-paraguay")
        analytics = scan(es, index=index_name, query={"query": {"match": {"descriptor.project.keyword": "wenet-ask-for-help-paraguay"}}})
        for analytic in analytics:
            analytic['_source']['descriptor']['project'] = "jFLFXPUDz4"
            index = analytic['_index']
            es.delete(index=index, id=analytic['_id'], doc_type=analytic['_type'])
            index = index.replace("wenet-ask-for-help-paraguay", "jflfxpudz4")
            es.index(index=index, id=analytic['_id'], doc_type=analytic['_type'], body=analytic['_source'])

        index_name = Utils.generate_index("analytic", project="wenet-ask-for-help-trento")
        analytics = scan(es, index=index_name, query={"query": {"match": {"descriptor.project.keyword": "wenet-ask-for-help-trento"}}})
        for analytic in analytics:
            analytic['_source']['descriptor']['project'] = "dLAIbwQczK"
            index = analytic['_index']
            es.delete(index=index, id=analytic['_id'], doc_type=analytic['_type'])
            index = index.replace("wenet-ask-for-help-trento", "dlaibwqczk")
            es.index(index=index, id=analytic['_id'], doc_type=analytic['_type'], body=analytic['_source'])

        index_name = Utils.generate_index("analytic", project="wenet-ask-for-help")
        analytics = scan(es, index=index_name, query={"query": {"match": {"descriptor.project.keyword": "wenet-ask-for-help"}}})
        for analytic in analytics:
            analytic['_source']['descriptor']['project'] = "xAcauSmrhd"
            index = analytic['_index']
            es.delete(index=index, id=analytic['_id'], doc_type=analytic['_type'])
            index = index.replace("wenet-ask-for-help", "xacausmrhd")
            es.index(index=index, id=analytic['_id'], doc_type=analytic['_type'], body=analytic['_source'])

    @property
    def action_name(self) -> str:
        return "project_to_app_id"

    @property
    def action_num(self) -> int:
        return 4
