# Copyright 2020 U-Hopper srl
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

from memex_logging.common.dao.message import MessageDao


class DaoCollector:
    """
    A collector of daos for the management of data
    """

    def __init__(self, message_dao: MessageDao) -> None:
        """
        :param MessageDao message_dao: a dao for the management of messages
        """

        self.message_dao = message_dao

    @staticmethod
    def build_dao_collector(es: Elasticsearch) -> DaoCollector:
        return DaoCollector(
            MessageDao(es)
        )
