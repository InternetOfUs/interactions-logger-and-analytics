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

from typing import Optional

from memex_logging.common.model.analytic.descriptor.builder import AnalyticDescriptorBuilder
from memex_logging.common.model.analytic.descriptor.common import CommonAnalyticDescriptor
from memex_logging.common.model.analytic.result.builder import AnalyticResultBuilder
from memex_logging.common.model.analytic.result.common import CommonAnalyticResult


class Analytic:

    def __init__(self, analytic_id: str, descriptor: CommonAnalyticDescriptor, result: Optional[CommonAnalyticResult] = None) -> None:
        self.analytic_id = analytic_id
        self.descriptor = descriptor
        self.result = result

    def to_repr(self) -> dict:
        return {
            'id': self.analytic_id,
            'query': self.descriptor.to_repr(),
            'result': self.result.to_repr() if self.result is not None else None
        }

    @staticmethod
    def from_repr(raw_data: dict) -> Analytic:
        descriptor = AnalyticDescriptorBuilder.from_repr(raw_data['query'])
        result = AnalyticResultBuilder.from_repr(raw_data['result']) if raw_data['result'] is not None else None
        return Analytic(raw_data['staticId'], descriptor, result=result)

    def __eq__(self, o) -> bool:
        if isinstance(o, Analytic):
            return o.analytic_id == self.analytic_id and o.descriptor == self.descriptor and o.result == self.result
        else:
            return False
