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


class AnalyticReorganizationMigration(MigrationAction):

    def apply(self, es: Elasticsearch) -> None:
        index_name = Utils.generate_index("analytic")
        analytics = scan(es, index=index_name, query={"query": {"match_all": {}}})

        for analytic in analytics:
            # Rename staticId key into id in the Analytic
            if "staticId" in analytic['_source']:
                analytic_id = analytic['_source'].pop("staticId")
                analytic['_source']['id'] = analytic_id

            # Rename query key into descriptor in the Analytic
            if "query" in analytic['_source']:
                descriptor = analytic['_source'].pop("query")
                analytic['_source']['descriptor'] = descriptor

            if analytic['_source']['descriptor']['type'] == "analytic":
                # Remove analytics with metric value:
                # - c:path
                # - c:length
                # - m:unhandled
                if analytic['_source']['descriptor']['metric'] in ["c:path", "c:length", "m:unhandled"]:
                    es.delete(index=analytic['_index'], id=analytic['_id'], doc_type=analytic['_type'])
                    continue

                # Change the analytic type into count or segmentation on the basis of the metric value.
                # The type should be converted into segmentation if metric value is among:
                # - a:segmentation
                # - g:segmentation
                # - m:segmentation
                # - r:segmentation
                # - u:segmentation
                # - t:segmentation
                # The type should be converted to count in all the other cases
                if analytic['_source']['descriptor']['metric'] in ["a:segmentation", "g:segmentation", "m:segmentation", "r:segmentation", "u:segmentation", "t:segmentation"]:
                    analytic['_source']['descriptor']['type'] = "segmentation"
                else:
                    analytic['_source']['descriptor']['type'] = "count"

                # Rename metric values to the new ones:
                # - u:total into total
                if analytic['_source']['descriptor']['metric'] == "u:total":
                    analytic['_source']['descriptor']['metric'] = "total"
                # - u:active into active
                if analytic['_source']['descriptor']['metric'] == "u:active":
                    analytic['_source']['descriptor']['metric'] = "active"
                # - u:engaged into engaged
                if analytic['_source']['descriptor']['metric'] == "u:engaged":
                    analytic['_source']['descriptor']['metric'] = "engaged"
                # - u:new into new
                if analytic['_source']['descriptor']['metric'] == "u:new":
                    analytic['_source']['descriptor']['metric'] = "new"
                # - a:segmentation into age
                if analytic['_source']['descriptor']['metric'] == "a:segmentation":
                    analytic['_source']['descriptor']['metric'] = "age"
                # - g:segmentation into gender
                if analytic['_source']['descriptor']['metric'] == "g:segmentation":
                    analytic['_source']['descriptor']['metric'] = "gender"
                # - m:from_users into from_users
                if analytic['_source']['descriptor']['metric'] == "m:from_users":
                    analytic['_source']['descriptor']['metric'] = "from_users"
                # - m:segmentation into all
                if analytic['_source']['descriptor']['metric'] == "m:segmentation":
                    analytic['_source']['descriptor']['metric'] = "all"
                # - r:segmentation into from_users
                if analytic['_source']['descriptor']['metric'] == "r:segmentation":
                    analytic['_source']['descriptor']['metric'] = "from_users"
                # - u:segmentation into from_users
                if analytic['_source']['descriptor']['metric'] == "u:segmentation":
                    analytic['_source']['descriptor']['metric'] = "from_users"
                # - m:from_bot into from_bot
                if analytic['_source']['descriptor']['metric'] == "m:from_bot":
                    analytic['_source']['descriptor']['metric'] = "from_bot"
                # - m:responses into responses
                if analytic['_source']['descriptor']['metric'] == "m:responses":
                    analytic['_source']['descriptor']['metric'] = "responses"
                # - m:notifications into notifications
                if analytic['_source']['descriptor']['metric'] == "m:notifications":
                    analytic['_source']['descriptor']['metric'] = "notifications"
                # - t:total into total
                if analytic['_source']['descriptor']['metric'] == "t:total":
                    analytic['_source']['descriptor']['metric'] = "total"
                # - t:active into active
                if analytic['_source']['descriptor']['metric'] == "t:active":
                    analytic['_source']['descriptor']['metric'] = "active"
                # - t:closed into closed
                if analytic['_source']['descriptor']['metric'] == "t:closed":
                    analytic['_source']['descriptor']['metric'] = "closed"
                # - t:new into new
                if analytic['_source']['descriptor']['metric'] == "t:new":
                    analytic['_source']['descriptor']['metric'] = "new"
                # - t:segmentation into label
                if analytic['_source']['descriptor']['metric'] == "t:segmentation":
                    analytic['_source']['descriptor']['metric'] = "label"
                # - c:total into total
                if analytic['_source']['descriptor']['metric'] == "c:total":
                    analytic['_source']['descriptor']['metric'] = "total"
                # - c:new into new
                if analytic['_source']['descriptor']['metric'] == "c:new":
                    analytic['_source']['descriptor']['metric'] = "new"
                # - d:fallback into fallback
                if analytic['_source']['descriptor']['metric'] == "d:fallback":
                    analytic['_source']['descriptor']['metric'] = "fallback"
                # - d:intents into intents
                if analytic['_source']['descriptor']['metric'] == "d:intents":
                    analytic['_source']['descriptor']['metric'] = "intents"
                # - d:domains into domains
                if analytic['_source']['descriptor']['metric'] == "d:domains":
                    analytic['_source']['descriptor']['metric'] = "domains"
                # - b:response into response
                if analytic['_source']['descriptor']['metric'] == "b:response":
                    analytic['_source']['descriptor']['metric'] = "response"

            if analytic['_source'].get('result') is not None:
                # Add the date fields to results using the correct range and as creation date the current one
                now = datetime.now()
                if analytic['_source']['result'].get('creationDt') is None:
                    analytic['_source']['result']['creationDt'] = now.isoformat()
                if analytic['_source']['result'].get('fromDt') is None or analytic['_source']['result'].get('toDt') is None:
                    timespan_type = analytic['_source']['descriptor']['timespan']['type']
                    if timespan_type == 'fixed':
                        analytic['_source']['result']['fromDt'] = analytic['_source']['descriptor']['timespan']['start']
                        analytic['_source']['result']['toDt'] = analytic['_source']['descriptor']['timespan']['end']
                    else:
                        analytic['_source']['result']['fromDt'] = now.isoformat()
                        analytic['_source']['result']['toDt'] = now.isoformat()

                # Remove from the analytic result the following keys:
                # - items
                # - transactions
                if 'items' in analytic['_source']['result']:
                    analytic['_source']['result'].pop('items')
                if 'transactions' in analytic['_source']['result']:
                    analytic['_source']['result'].pop('transactions')

                # Renamed counts key in segments in the SegmentationResult
                if 'counts' in analytic['_source']['result']:
                    segments = analytic['_source']['result'].pop('counts')
                    analytic['_source']['result']['segments'] = segments

                # Changed older segments structure into the new one
                if 'segments' in analytic['_source']['result']:
                    if isinstance(analytic['_source']['result']['segments'], dict):
                        segments = []
                        for key in analytic['_source']['result']['segments']:
                            segments.append({'count': analytic['_source']['result']['segments'].pop('key'), 'type': key})
                        analytic['_source']['result']['segments'] = segments

                # Align analytic descriptor and result types
                if analytic['_source']['descriptor']['type'] == "count":
                    analytic['_source']['result']['type'] = "count"
                if analytic['_source']['descriptor']['type'] == "segmentation":
                    analytic['_source']['result']['type'] = "segmentation"
                if analytic['_source']['descriptor']['type'] == "aggregation":
                    analytic['_source']['result']['type'] = "aggregation"

                # Change the structure of the AggregationResult
                if "avg" in analytic['_source']['result']:
                    aggregation = analytic['_source']['result'].pop('avg')
                    analytic['_source']['result']['aggregation'] = aggregation
                if "min" in analytic['_source']['result']:
                    aggregation = analytic['_source']['result'].pop('min')
                    analytic['_source']['result']['aggregation'] = aggregation
                if "max" in analytic['_source']['result']:
                    aggregation = analytic['_source']['result'].pop('max')
                    analytic['_source']['result']['aggregation'] = aggregation
                if "sum" in analytic['_source']['result']:
                    aggregation = analytic['_source']['result'].pop('sum')
                    analytic['_source']['result']['aggregation'] = aggregation
                if "stats" in analytic['_source']['result']:
                    aggregation = analytic['_source']['result'].pop('stats')
                    analytic['_source']['result']['aggregation'] = aggregation
                if "extended_stats" in analytic['_source']['result']:
                    aggregation = analytic['_source']['result'].pop('extended_stats')
                    analytic['_source']['result']['aggregation'] = aggregation
                if "value_count" in analytic['_source']['result']:
                    aggregation = analytic['_source']['result'].pop('value_count')
                    analytic['_source']['result']['aggregation'] = aggregation
                if "cardinality" in analytic['_source']['result']:
                    aggregation = analytic['_source']['result'].pop('cardinality')
                    analytic['_source']['result']['aggregation'] = aggregation
                if "percentiles" in analytic['_source']['result']:
                    aggregation = analytic['_source']['result'].pop('percentiles')
                    analytic['_source']['result']['aggregation'] = aggregation

            es.index(index=analytic['_index'], id=analytic['_id'], doc_type=analytic['_type'], body=analytic['_source'])

    @property
    def action_name(self) -> str:
        return "analytic_reorganization"

    @property
    def action_num(self) -> int:
        return 3
