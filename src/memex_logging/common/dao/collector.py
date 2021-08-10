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

from memex_logging.common.dao.analytic import AnalyticDao
from memex_logging.common.dao.message import MessageDao


class DaoCollector:
    """
    A collector of daos for the management of data
    """

    def __init__(self, message_dao: MessageDao, analytic_dao: AnalyticDao) -> None:
        """
        :param MessageDao message_dao: the message dao
        :param AnalyticDao analytic_dao: the analytic dao
        """

        self.message = message_dao
        self.analytic = analytic_dao

    @staticmethod
    def build_dao_collector(es: Elasticsearch) -> DaoCollector:
        return DaoCollector(
            MessageDao(es),
            AnalyticDao(es)
        )
