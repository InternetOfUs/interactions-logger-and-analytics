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
from typing import Tuple

from elasticsearch import Elasticsearch

from memex_logging.common.dao.common import CommonDao, DocumentNotFound
from memex_logging.common.model.analytic.analytic import Analytic


logger = logging.getLogger("logger.common.dao.analytic")


class AnalyticDao(CommonDao):

    BASE_INDEX = "analytic"

    def __init__(self, es: Elasticsearch) -> None:
        """
        :param Elasticsearch es: a connector for Elasticsearch
        """
        super().__init__(es, self.BASE_INDEX)

    @staticmethod
    def _build_query_by_analytic_id(analytic_id: str) -> dict:
        return {
            "query": {
                "match": {
                    "id.keyword": analytic_id
                }
            }
        }

    def add(self, analytic: Analytic, doc_type: str = "_doc") -> None:
        """
        Add an analytic to Elasticsearch

        :param Analytic analytic: the analytic to add
        :param str doc_type: the type of the document
        """

        index = self._generate_index(dt=datetime.now())
        self._add_document(index, analytic.to_repr(), doc_type=doc_type)

    def update(self, index: str, trace_id: str, analytic: Analytic, doc_type: str = "_doc") -> None:
        """
        Update an analytic in Elasticsearch

        :param str index: the index where to add the document
        :param str trace_id: the id of the document
        :param Analytic analytic: the updated analytic
        :param str doc_type: the type of the document
        """

        self._update_document(index, trace_id, analytic.to_repr(), doc_type=doc_type)

    def get(self, analytic_id: str) -> Analytic:
        """
        Retrieve an analytic from Elasticsearch specifying the `analytic_id`

        :param str analytic_id: the id of the analytic to retrieve
        :return: the analytic
        :raise EntryNotFound: when could not find any analytic
        """

        index = self._generate_index()
        query = self._build_query_by_analytic_id(analytic_id)
        raw_documents = self._search_documents(index, query)
        if len(raw_documents) == 0:
            raise DocumentNotFound(f"Analytic with id [{analytic_id}] was not found")
        elif len(raw_documents) > 1:
            logger.warning(f"More than one analytic with id [{analytic_id}] was found")

        return Analytic.from_repr(raw_documents[0])

    def get_with_additional_information(self, analytic_id: str) -> Tuple[Analytic, str, str, str]:
        """
        Retrieve an analytic from Elasticsearch specifying the `analytic_id` and also trace id, index and doc type

        :param str analytic_id: the id of the analytic to retrieve
        :return: a tuple containing the analytic, trace_id, index and doc type
        :raise EntryNotFound: when could not find any analytic
        """

        index = self._generate_index()
        query = self._build_query_by_analytic_id(analytic_id)
        raw_analytic, trace_id, index, doc_type = self._get_document(index, query)
        analytic = Analytic.from_repr(raw_analytic)
        return analytic, trace_id, index, doc_type

    def delete(self, analytic_id: str) -> None:
        """
        Delete an analytic from Elasticsearch specifying the `analytic_id`

        :param str analytic_id: the id of the analytic to delete
        """

        index = self._generate_index()
        query = self._build_query_by_analytic_id(analytic_id)
        self._delete_document(index, query)
