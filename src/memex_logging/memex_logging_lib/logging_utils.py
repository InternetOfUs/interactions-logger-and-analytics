import json
import requests


class LoggingUtility:

    def __init__(self, service_host: str, service_port: int):
        self._access_point = service_host + ":" + str(service_port)

    def add_message(self, message: dict) -> tuple:
        """
        Utils to add a single message to the database
        :param message: the message to be added
        :return: the HTTP response
        """
        if not isinstance(message, dict):
            raise ValueError("message should be a dictionary")

        api_point = self._access_point + "/message"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=message)

        return response.status_code, response.content

    def add_messages(self, messages: list) -> tuple:
        """
        Utils to add a list of messages to the database
        :param messages: the list of messages to store
        :return: the HTTP response
        """
        if not isinstance(messages, list):
            raise  ValueError("messages should be a list")
        api_point = self._access_point + "/messages"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=messages)

        return response.status_code, response.content

    def add_log(self, log: dict) -> tuple:
        """
        Utils to add a generic log to the database
        :param log: the log to be added
        :return: the HTTP response
        """
        if not isinstance(log, dict):
            raise ValueError("log should be a dictionary")

        api_point = self._access_point + "/log"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=log)

        return response.status_code, response.content

    def add_logs(self, logs: list) -> tuple:
        """
        Utils to add a list of messages to the database
        :param logs: the list of logs to store
        :return: the HTTP response
        """
        if not isinstance(logs, list):
            raise ValueError("logs should be a list")

        api_point = self._access_point + "/logs"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=logs)

        return response.status_code, response.content

    def get_message(self, trace_id: str) -> tuple:
        """
        Utils to retrieve a message from the database
        :param trace_id: the if of the message to retrieve
        :return: the status and the content of the response
        """
        api_point = self._access_point + "/performance/message/" + trace_id

        response = requests.get(api_point)

        return response.status_code, response.json()

    def get_conversation(self, conversation_id: str) -> tuple:
        """
        Utils to retrieve a conversation from the database
        :param conversation_id: the id of the conversation
        :return: the status and the content of the response
        """

        api_point = self._access_point + "/performance/conversation/" + conversation_id

        response = requests.get(api_point)

        return response.status_code, response.json()
