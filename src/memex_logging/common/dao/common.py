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
from typing import Optional, Tuple, List

from elasticsearch import Elasticsearch

from memex_logging.common.utils import Utils


logger = logging.getLogger("logger.common.dao.common")


class DocumentNotFound(Exception):
    pass


class CommonDao:

    def __init__(self, es: Elasticsearch, base_index: str) -> None:
        self._es = es
        self._base_index = base_index

    def _generate_index(self, dt: Optional[datetime] = None) -> str:
        """
        Generate the Elasticsearch index associated to the message, the format is `data_type-%Y-%m-%d`.

        :param Optional[datetime] dt: the datetime of the message
        :return: the generated Elasticsearch index
        :raise ValueError: when there is a datetime but not a project
        """

        return Utils.generate_index(self._base_index, dt=dt)

    @staticmethod
    def _build_query_by_id(trace_id: str) -> dict:
        return {
            "size": 1,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "_id": trace_id
                            }
                        }
                    ]
                }
            }
        }

    @staticmethod
    def _build_time_range_query(from_time: datetime, to_time: datetime, max_size: int) -> dict:
        return {
            "size": max_size,
            "query": {
                "bool": {
                    "must": [
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": from_time.isoformat(),
                                    "lte": to_time.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "timestamp": {
                        "order": "asc"
                    }
                }
            ]
        }

    @staticmethod
    def _add_project_to_query(query: dict, project: str) -> dict:
        query["query"]["bool"]["must"].append(
            {
                "match_phrase": {
                    "project.keyword": project
                }
            }
        )
        return query

    def _add_document(self, index: str, object_repr: dict, doc_type: str = "_doc") -> str:
        """
        Add a document to Elasticsearch

        :param str index: the index where to add the document
        :param dict object_repr: the document to add
        :param str doc_type: the type of the document
        :return: the trace_id of the added representation of an object
        """

        query = self._es.index(index=index, body=object_repr, doc_type=doc_type)
        return query["_id"]

    def _update_document(self, index: str, trace_id: str, object_repr: dict, doc_type: str = "_doc") -> None:
        """
        Add a document to Elasticsearch

        :param str index: the index where to add the document
        :param str trace_id: the id of the document
        :param dict object_repr: the document to add
        :param str doc_type: the type of the document
        """

        self._es.index(index=index, id=trace_id, doc_type=doc_type, body=object_repr)

    def _get_document(self, index: str, query: dict) -> Tuple[dict, str, str, str]:
        """
        Retrieve a document from Elasticsearch

        :param str index: the index from which to retrieve the document
        :param dict query: the query for retrieving the document
        :return: a tuple containing the document and the trace_id of that document
        :raise EntryNotFound: when could not find any document
        """

        response = self._es.search(index=index, body=query)
        if len(response['hits']['hits']) == 0:
            logger.debug("Could not find any document")
            raise DocumentNotFound(f"No document was found")
        elif len(response['hits']['hits']) > 1:
            logger.warning(f"More than one document was found")

        return response['hits']['hits'][0]['_source'], response['hits']['hits'][0]['_id'], response['hits']['hits'][0]['_index'], response['hits']['hits'][0]['_type']

    def _delete_document(self, index: str, query: dict) -> None:
        """
        Delete a document from Elasticsearch

        :param str index: the index from which to delete the document
        :param dict query: the query for deleting the document
        """

        self._es.delete_by_query(index=index, body=query)

    def _search_documents(self, index: str, query: dict) -> List[dict]:
        """
        Search documents in Elasticsearch

        :param str index: the target index for the search
        :param dict query: the search query
        :return: the list of documents matching the query
        """

        response = self._es.search(index=index, body=query)
        return [element['_source'] for element in response['hits']['hits']]
