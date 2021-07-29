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

import logging
from datetime import datetime

from elasticsearch import Elasticsearch
from wenet.interface.wenet import WeNet
from wenet.model.user.common import Gender

from memex_logging.common.model.analytic.descriptor.segmentation import SegmentationDescriptor, \
    UserSegmentationDescriptor, MessageSegmentationDescriptor, TransactionSegmentationDescriptor
from memex_logging.common.model.analytic.result.segmentation import SegmentationResult, Segmentation
from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.common.analytic.analytic")


class SegmentationComputation:

    def __init__(self, es: Elasticsearch, wenet_interface: WeNet) -> None:
        self.es = es
        self.wenet_interface = wenet_interface

    def get_result(self, analytic: SegmentationDescriptor) -> SegmentationResult:
        if isinstance(analytic, UserSegmentationDescriptor):
            if analytic.metric.lower() == "age":
                result = self._user_age_segmentation(analytic)
            elif analytic.metric.lower() == "gender":
                result = self._user_gender_segmentation(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for SegmentationDescriptor")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for SegmentationDescriptor")

        elif isinstance(analytic, MessageSegmentationDescriptor):
            if analytic.metric.lower() == "all":
                result = self._messages_segmentation(analytic)
            elif analytic.metric.lower() == "from_users":
                result = self._user_messages_segmentation(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for MessageSegmentationDescriptor")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for MessageSegmentationDescriptor")

        elif isinstance(analytic, TransactionSegmentationDescriptor):
            if analytic.metric.lower() == "label":
                result = self._transactions_segmentation(analytic)
            else:
                logger.info(f"Unknown value for metric [{analytic.metric}] for TransactionSegmentationDescriptor")
                raise ValueError(f"Unknown value for metric [{analytic.metric}] for TransactionSegmentationDescriptor")

        else:
            logger.info(f"Unrecognized class of SegmentationDescriptor [{type(analytic)}]")
            raise ValueError(f"Unrecognized class of SegmentationDescriptor [{type(analytic)}]")

        return result

    def _user_age_segmentation(self, analytic: UserSegmentationDescriptor) -> SegmentationResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        user_ids = self.wenet_interface.hub.get_user_ids_for_app(analytic.project, from_datetime=min_bound, to_datetime=max_bound)
        ages = []
        for user_id in user_ids:
            user_profile = self.wenet_interface.profile_manager.get_user_profile(user_id)
            ages.append(Utils.compute_age(user_profile.date_of_birth.date_dt) if user_profile.date_of_birth is not None and user_profile.date_of_birth.date_dt is not None else None)

        type_counter = [
            Segmentation("0-18", len([age for age in ages if age is not None and 0 <= age <= 18])),
            Segmentation("19-25", len([age for age in ages if age is not None and 19 <= age <= 25])),
            Segmentation("26-35", len([age for age in ages if age is not None and 26 <= age <= 35])),
            Segmentation("36-45", len([age for age in ages if age is not None and 36 <= age <= 45])),
            Segmentation("46-55", len([age for age in ages if age is not None and 46 <= age <= 55])),
            Segmentation("55+", len([age for age in ages if age is not None and 55 < age])),
            Segmentation("unavailable", ages.count(None))
        ]
        return SegmentationResult(type_counter, datetime.now(), min_bound, max_bound)

    def _user_gender_segmentation(self, analytic: UserSegmentationDescriptor) -> SegmentationResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        user_ids = self.wenet_interface.hub.get_user_ids_for_app(analytic.project, from_datetime=min_bound, to_datetime=max_bound)
        genders = []
        for user_id in user_ids:
            user_profile = self.wenet_interface.profile_manager.get_user_profile(user_id)
            genders.append(user_profile.gender)

        type_counter = [
            Segmentation("male", genders.count(Gender.MALE)),
            Segmentation("female", genders.count(Gender.FEMALE)),
            Segmentation("non-binary", genders.count(Gender.NON_BINARY)),
            Segmentation("in-another-way", genders.count(Gender.OTHER)),
            Segmentation("prefer-not-to-say", genders.count(Gender.NOT_SAY)),
            Segmentation("unavailable", genders.count(None))
        ]
        return SegmentationResult(type_counter, datetime.now(), min_bound, max_bound)

    def _messages_segmentation(self, analytic: MessageSegmentationDescriptor) -> SegmentationResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat(),
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "type.keyword",
                        "size": 5
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        type_counter = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                type_counter.append(Segmentation(item['key'], item['doc_count']))

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `5` but the number of types is higher")

        return SegmentationResult(type_counter, datetime.now(), min_bound, max_bound)

    def _user_messages_segmentation(self, analytic: MessageSegmentationDescriptor) -> SegmentationResult:
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type.keyword": "request"
                            }
                        },
                        {
                            "match": {
                                "project.keyword": analytic.project
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": min_bound.isoformat(),
                                    "lte": max_bound.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "terms_count": {
                    "terms": {
                        "field": "content.type.keyword",
                        "size": 10
                    }
                }
            }
        }

        index = Utils.generate_index(data_type="message", project=analytic.project)
        response = self.es.search(index=index, body=body, size=0)
        type_counter = []
        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'buckets' in response['aggregations']['terms_count']:
            for item in response['aggregations']['terms_count']['buckets']:
                type_counter.append(Segmentation(item['key'], item['doc_count']))

        if 'aggregations' in response and 'terms_count' in response['aggregations'] and 'sum_other_doc_count' in response['aggregations']['terms_count']:
            if response['aggregations']['terms_count']['sum_other_doc_count'] != 0:
                logger.warning("The number of buckets is limited at `10` but the number of content types is higher")

        return SegmentationResult(type_counter, datetime.now(), min_bound, max_bound)

    def _transactions_segmentation(self, analytic: TransactionSegmentationDescriptor) -> SegmentationResult:
        """
        Compute the segmentation of the transactions of a given application (analytic.project) in a given time range (analytic.timespan).
        The computation of that count is done segmenting based on the labels of the transactions created in the time range.
        Optionally if specified a task identifier the transactions are only relative to that task.
        """
        min_bound, max_bound = Utils.extract_range_timestamps(analytic.time_span)
        transactions = self.wenet_interface.task_manager.get_all_transactions(app_id=analytic.project, creation_from=min_bound, creation_to=max_bound,  task_id=analytic.task_id)
        transaction_labels = [transaction.label for transaction in transactions]
        unique_labels = set(transaction_labels)
        type_counter = [Segmentation(label, transaction_labels.count(label)) for label in unique_labels]
        return SegmentationResult(type_counter, datetime.now(), min_bound, max_bound)
