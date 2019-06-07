import requests


class LoggingUtility:

    def __init__(self, service_host: str, service_port: int, project:str):
        self._access_point = service_host + ":" + str(service_port)
        self._project = project

    def add_messages(self, messages: list) -> tuple:
        """
        Utils to add a list of messages to the database
        :param messages: the list of messages to store
        :return: the HTTP response
        """
        if not isinstance(messages, list):
            raise ValueError("messages should be a list")
        api_point = self._access_point + "/messages"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=messages)

        return response.status_code, response.content

    def get_message(self, message_id: str) -> tuple:
        """
        Utils to retrieve a message from the database
        :param message_id: the if of the message to retrieve
        :return: the status and the content of the response
        """
        api_point = self._access_point + "/message/" + self._project + "/" + message_id

        response = requests.get(api_point)

        return response.status_code, response.json()

    def delete_message(self, message_id:str) -> tuple:
        """
        Utils to delete a message
        :param message_id:
        :return: the HTTP response
        """
        api_point = self._access_point + "/message/" + self._project + "/" + message_id

        response = requests.delete(api_point)

        return response.status_code, response.json()
