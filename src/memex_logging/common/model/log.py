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


class Log:

    def __init__(self, log_id: str, project: str, component: str, authority: str, severity: str, log_content: str,
                 timestamp: str, bot_version: str, metadata: dict) -> None:
        self.id = log_id
        self.project = project
        self.component = component
        self.authority = authority
        self.severity = severity
        self.log_content = log_content
        self.timestamp = timestamp
        self.bot_version = bot_version
        self.metadata = metadata

    def to_repr(self) -> dict:
        return {
            'logId': self.id,
            'project': self.project,
            'component': self.component,
            'authority': self.authority,
            'severity': self.severity,
            'logContent': self.log_content,
            'timestamp': self.timestamp,
            'botVersion': self.bot_version,
            'metadata': self.metadata
        }

    @staticmethod
    def from_repr(raw_data: dict) -> Log:
        authority = None
        if 'authority' in raw_data:
            authority = raw_data['authority']

        bot_version = None
        if 'botVersion' in raw_data:
            bot_version = raw_data['botVersion']

        metadata = None
        if 'metadata' in raw_data:
            metadata = raw_data['metadata']

        return Log(raw_data['logId'], raw_data['project'], raw_data['component'], authority,
                   raw_data['severity'], raw_data['logContent'], raw_data['timestamp'], bot_version, metadata)
