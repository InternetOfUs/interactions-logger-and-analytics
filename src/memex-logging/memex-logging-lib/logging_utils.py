import json
import requests


class LoggingUtility:

    def add_message(message: dict, service_host: str = "http://localhost", service_port: int =5000) -> tuple:
        """
        Utils to add a message to the database
        :param service_host: location of the service - default localhost
        :param service_port: port of the service - default 5000
        :return: the status code and the content of the response
        """
        api_point = service_host + ":" + str(service_port) + "/message"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=message)

        return response.status_code, response.content

    def add_messages(messages: list, service_host: str = "http://localhost", service_port: int =5000) -> tuple:
        """
        Utils to add a list of messages to the database
        :param service_host: location of the service - default localhost
        :param service_port: port of the service - default 5000
        :return: the status and the content of the response
        """

        api_point = service_host + ":" + str(service_port) + "/message"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=messages)

        return response.status_code, response.content

    def add_log(message: dict, service_host: str = "http://localhost", service_port: int =5000) -> tuple:
        """
        Utils to add a generic log to the database
        :param service_host: location of the service - default localhost
        :param service_port: port of the service - default 5000
        :return: the status and the content of the response
        """
        api_point = service_host + ":" + str(service_port) + "/log"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=message)

        return response.status_code, response.content

    def add_logs(messages: list, service_host: str = "http://localhost", service_port: int =5000) -> tuple:
        """
        Utils to add an array of logs to the database
        :param service_host: location of the service - default localhost
        :param service_port: port of the service - default 5000
        :return: the status and the content of the response
        """
        api_point = service_host + ":" + str(service_port) + "/message"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=messages)

        return response.status_code, response.content

    def get_message(trace_id: str, service_host: str = "http://localhost", service_port: int =5000) -> tuple:
        """
        Utils to retrieve a message from the database
        :param service_host: location of the service - default localhost
        :param service_port: port of the service - default 5000
        :return: the status and the content of the response
        """
        api_point = service_host + ":" + str(service_port) + "/message/" + trace_id

        response = requests.get(api_point)

        return response.status_code, response.content

    def get_conversation(conversation_id: str, service_host: str = "http://localhost", service_port: int =5000) -> tuple:
        """
        Utils to retrieve
        :param service_host: location of the service - default localhost
        :param service_port: port of the service - default 5000
        :return: the status and the content of the response
        """
        api_point = service_host + ":" + str(service_port) + "/conversation/" + conversation_id

        response = requests.get(api_point)

        return response.status_code, response.content
