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
from typing import Optional

from elasticsearch import Elasticsearch
from wenet.interface.wenet import WeNet

from memex_logging.common.computation.aggregation import AggregationComputation
from memex_logging.common.computation.count import CountComputation
from memex_logging.common.computation.segmentation import SegmentationComputation
from memex_logging.common.model.analytic.descriptor.aggregation import AggregationDescriptor
from memex_logging.common.model.analytic.descriptor.common import CommonAnalyticDescriptor
from memex_logging.common.model.analytic.descriptor.count import CountDescriptor
from memex_logging.common.model.analytic.descriptor.segmentation import SegmentationDescriptor
from memex_logging.common.model.analytic.result.common import CommonAnalyticResult


logger = logging.getLogger("logger.common.analytic.analytic")


class AnalyticComputation:

    def __init__(self, es: Elasticsearch, wenet_interface: WeNet) -> None:
        self.es = es
        self.wenet_interface = wenet_interface

    def get_result(self, analytic: CommonAnalyticDescriptor) -> Optional[CommonAnalyticResult]:
        if isinstance(analytic, CountDescriptor):
            count_computation = CountComputation(self.es, self.wenet_interface)
            result = count_computation.get_result(analytic)

        elif isinstance(analytic, SegmentationDescriptor):
            segmentation_computation = SegmentationComputation(self.es, self.wenet_interface)
            result = segmentation_computation.get_result(analytic)

        elif isinstance(analytic, AggregationDescriptor):
            aggregation_computation = AggregationComputation(self.es)
            result = aggregation_computation.get_result(analytic)

        else:
            logger.info(f"Unrecognized class of AnalyticDescriptor [{type(analytic)}]")
            raise ValueError(f"Unrecognized class of AnalyticDescriptor [{type(analytic)}]")

        return result
