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

from typing import Union

from memex_logging.common.model.aggregation import Aggregation
from memex_logging.common.model.analytic import CommonAnalytic


class AnalyticBuilder:

    @staticmethod
    def from_repr(raw_data: dict) -> Union[CommonAnalytic, Aggregation]:
        if 'type' in raw_data:
            if str(raw_data['type']).lower() == CommonAnalytic.ANALYTIC_TYPE:
                return CommonAnalytic.from_repr(raw_data)
            elif str(raw_data['type']).lower() == Aggregation.AGGREGATION_TYPE:
                return Aggregation.from_repr(raw_data)
            else:
                raise ValueError("Unrecognized dimension for Analytic")
        else:
            raise ValueError("An Analytic must contain a dimension")
