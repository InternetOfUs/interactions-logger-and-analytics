from __future__ import absolute_import, annotations

from unittest import TestCase

from memex_logging.ws.ws import WsInterface
from test.unit.memex_logging.ws.common.mock.daos import MockDaoCollectorBuilder


class CommonWsTestCase(TestCase):
    """
    A common test case for the smart-places web service resources
    """

    def setUp(self) -> None:
        super().setUp()
        self.dao_collector = MockDaoCollectorBuilder.build_mock_daos()
        api = WsInterface(self.dao_collector, None)
        api.get_application().testing = True
        self.client = api.get_application().test_client()
