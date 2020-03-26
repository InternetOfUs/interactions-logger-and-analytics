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

from __future__ import annotations, absolute_import

import logging


class Log:

    def __init__(self, log_id: str, project: str, component: str, authority: str, severity: str, log_content: str, timestamp: str, bot_version: str, metadata: dict) -> None:
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
    def from_rep(data: dict) -> Log:

        if 'logId' not in data:
            logging.error("MODEL.LOG logId must be defined")
            raise ValueError("logId must be defined")

        if 'project' not in data:
            logging.error("MODEL.LOG project must be defined")
            raise ValueError("project must be defined")

        if 'component' not in data:
            logging.error("MODEL.LOG component must be defined")
            raise ValueError("component must be defined")

        if 'severity' not in data:
            logging.error("MODEL.LOG severity must be defined")
            raise ValueError("severity must be defined")

        if 'logContent' not in data:
            logging.error("MODEL.LOG logContent must be defined")
            raise ValueError("logContent must be defined")

        if 'timestamp' not in data:
            logging.error("MODEL.LOG timestamp must be defined")
            raise ValueError("timestamp must be defined")

        bot_version = None
        metadata = None
        authority = None

        if 'authority' in data:
            authority = data['authority']

        if 'botVersion' in data:
            bot_version = data['botVersion']

        if 'metadata' in data:
            metadata = data['metadata']

        return Log(data['logId'],str(data['project']).lower(),str(data['component']).lower(),authority, str(data['severity']).upper(),data['logContent'],data['timestamp'],bot_version,metadata)
