import logging
from typing import Optional

from elasticsearch import Elasticsearch

from memex_logging.common.dao.common import CommonDao, EntryNotFound
from memex_logging.common.model.analytic.analytic import Analytic
from memex_logging.common.utils import Utils


class AnalyticDao(CommonDao):
    # TODO check: is this the correct value?
    BASE_INDEX = "analytic"

    def __init__(self, es: Elasticsearch) -> None:
        """
        :param Elasticsearch es: a connector for Elasticsearch
        """
        super().__init__(es, self.BASE_INDEX)

    # TODO should be renamed to `get`
    # TODO should
    def get_analytic(self, analytic_id: str, project: Optional[str] = None) -> Analytic:
        query = {"query": {"match": {"id.keyword": analytic_id}}}
        index_name = Utils.generate_index("analytic", project=project)
        raw_documents = self.search(index_name, query)
        if len(raw_documents) == 0:
            raise EntryNotFound(f"Analytic with id [{analytic_id}] was not found")
        elif len(raw_documents) > 1:
            logging.warning(f"More than one analytic with id [{analytic_id}] was found")

        return Analytic.from_repr(raw_documents[0])
