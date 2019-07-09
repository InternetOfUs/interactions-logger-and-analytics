from __future__ import annotations, absolute_import

import logging


class Log:

    def __init__(self, log_id: str, severity: str, log_content: str, timestamp: str, authority: str, resolution: dict, message_id: str, bot_version: str, device: dict, metadata: dict, project: str) -> None:
        self.id = log_id
        self.severity = severity
        self.log_content = log_content
        self.timestamp = timestamp
        self.authority = authority
        self.resolution = resolution
        self.message_id = message_id
        self.bot_version = bot_version
        self.device = device
        self.metadata = metadata
        self.project = project

    def to_repr(self) -> dict:
        return {
            'logId': self.id,
            'severity': self.severity,
            'logContent': self.log_content,
            'timestamp': self.timestamp,
            'authority': self.authority,
            'resolution': self.resolution,
            'messageId': self.message_id,
            'botVersion': self.bot_version,
            'device': self.device,
            'metadata': self.metadata,
            'project': self.project
        }

    @staticmethod
    def from_rep(data: dict) -> Log:

        if 'severity' not in data:
            logging.error("MODEL.LOG severity must be defined")
            raise ValueError("severity must be defined")

        if 'logContent' not in data:
            logging.error("MODEL.LOG logContent must be defined")
            raise ValueError("logContent must be defined")

        if 'timestamp' not in data:
            logging.error("MODEL.LOG timestamp must be defined")
            raise ValueError("timestamp must be defined")

        if 'authority' not in data:
            logging.error("MODEL.LOG timestamp must be defined")
            raise ValueError("timestamp must be defined")

        if 'logId' not in data:
            logging.error("MODEL.LOG logId must be defined")
            raise ValueError("logId must be defined")

        if 'project' not in data:
            logging.error("MODEL.LOG project must be defined")
            raise ValueError("project must be defined")

        message_id = None
        resolution = None
        bot_version = None
        device = None
        metadata = None

        if 'resolution' in data:
            if isinstance(data['resolution'], dict):
                resolution = data['resolution']
            else:
                logging.error("resolution should be a dict")
                raise ValueError("resolution should be a dict")

        if 'botVersion' in data:
            if isinstance(data['botVersion'], str):
                bot_version = data['botVersion']
            else:
                logging.error("botVerion should be a string")
                raise ValueError("botVerion should be a string")

        if 'messageId' in data:
            if isinstance(data['messageId'], str):
                bot_version = data['messageId']
            else:
                logging.error("messageId should be a string")
                raise ValueError("messageId should be a string")

        if 'device' in data:
            if isinstance(data['device'], dict):
                device = data['device']
            else:
                logging.error("device should be a dict")
                raise ValueError("device should be a dict")

        if 'metadata' in data:
            if isinstance(data['metadata'], dict):
                metadata = data['metadata']
            else:
                logging.error("metadata should be a dict")
                raise ValueError("metadata should be a dict")

        return Log(data['logId'], data['severity'], data['logContent'], data['timestamp'], data['authority'], resolution, message_id, bot_version, device, metadata, data['project'])
