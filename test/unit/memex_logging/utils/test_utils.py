from __future__ import absolute_import, annotations

from datetime import datetime
from unittest import TestCase

from memex_logging.utils.utils import Utils


class TestUtils(TestCase):

    def test_generate_index(self):
        index = Utils.generate_index("data_type")
        self.assertEqual("data_type-*", index)

        index = Utils.generate_index("data_type", project="project")
        self.assertEqual("data_type-project-*", index)

        with self.assertRaises(ValueError):
            Utils.generate_index("data_type", dt=datetime(2021, 2, 5))

        index = Utils.generate_index("data_type", project="project", dt=datetime(2021, 2, 5))
        self.assertEqual("data_type-project-2021-02-05", index)
