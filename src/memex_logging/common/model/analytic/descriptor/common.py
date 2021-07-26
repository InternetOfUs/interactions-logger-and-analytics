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

from abc import ABC, abstractmethod

from memex_logging.common.model.time import TimeWindow


class CommonAnalyticDescriptor(ABC):

    def __init__(self, time_span: TimeWindow, project: str) -> None:
        self.time_span = time_span
        self.project = project

    @abstractmethod
    def to_repr(self) -> dict:
        pass

    @staticmethod
    @abstractmethod
    def from_repr(raw_data: dict) -> CommonAnalyticDescriptor:
        pass
