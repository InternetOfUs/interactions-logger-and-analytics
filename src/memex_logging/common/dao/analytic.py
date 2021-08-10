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

from elasticsearch import Elasticsearch

from memex_logging.common.dao.common import CommonDao, EntryNotFound
from memex_logging.common.model.analytic.analytic import Analytic
from memex_logging.common.utils import Utils


class AnalyticDao(CommonDao):

    BASE_INDEX = "analytic"

    def __init__(self, es: Elasticsearch) -> None:
        """
        :param Elasticsearch es: a connector for Elasticsearch
        """
        super().__init__(es, self.BASE_INDEX)

    # TODO should be renamed to `get`
    def get_analytic(self, analytic_id: str) -> Analytic:
        query = {"query": {"match": {"id.keyword": analytic_id}}}
        index_name = Utils.generate_index("analytic")
        raw_documents = self.search(index_name, query)
        if len(raw_documents) == 0:
            raise EntryNotFound(f"Analytic with id [{analytic_id}] was not found")
        elif len(raw_documents) > 1:
            logging.warning(f"More than one analytic with id [{analytic_id}] was found")

        return Analytic.from_repr(raw_documents[0])
