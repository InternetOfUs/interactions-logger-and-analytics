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

import json
import logging
from datetime import datetime
from time import sleep
from typing import Optional, List

import requests

from memex_logging.common.model.analytic.descriptor.common import CommonAnalyticDescriptor
from memex_logging.common.model.message import Entity, ActionResponse, CarouselCardResponse


logger = logging.getLogger("logger.memex_logging_lib.logging_utils")


class LoggingUtility:

    def __init__(self, service_host: str, project: str, custom_headers: Optional[dict] = None) -> None:
        """
        Initialize a logging Utility object by specifying the host, the port and the project
        :param service_host: the host of the service as a string (e.g, https://www.test.com)
        :param project: the name of the project. It is used to create well-formed indexes in the database and to retrieve information from the right index
        :param custom_headers: a dictionary containing some custom headers
        """
        self._access_point = service_host
        self._project = project
        self._custom_headers = custom_headers if custom_headers else {}

    def add_location_request(self, latitude: float, longitude: float, message_id: str, user_id: str, channel: str,
                             timestamp: str, conversation_id: str = None, domain: str = None, intent_name: str = None,
                             intent_confidence: float = None,
                             entities: object = Optional[List[Entity]], language: object = None, metadata: object = Optional[dict]) -> str:
        """
        This method should be used to store a request message that contains text written by the user. A request message is a message from the user to the chatbot
        :param latitude: a float number representing the latitude of the location
        :param longitude a float number representing the longitude of the location
        :param message_id: the id of the message. This field should be a string
        :param user_id: the id of the user. This filed should be a string
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param timestamp: the timestamp of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param domain: this optional field is a string that describe the domain of the message
        :param intent_name: it is an optional string representing the name of the intent
        :param intent_confidence: it is an optional float number to measure the intent detection accuracy. If intent_name is defined, the default value of the confidence is 0.0
        :param entities: an optional list of entities. Each entity should have type Entity
        :param language: an optional string indicating the language of the message
        :param metadata: an optional dictionary containing additional information.
        :return:
        """

        if message_id is None or message_id == "":
            raise ValueError("message_id cannot be empty")

        if user_id is None or user_id == "":
            raise ValueError("user_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if timestamp is None or timestamp == "":
            raise ValueError("timestamp cannot be empty")

        if not isinstance(entities, list):
            entities = []

        if not isinstance(metadata, dict):
            metadata = {}

        api_point = self._access_point + "/messages"

        intent_dict = {
            'name': intent_name,
            'confidence': intent_confidence
        }

        entity_list = []
        for entity in entities:
            entity_list.append(entity.to_repr())

        message_generated = {
            "messageId": message_id,
            "channel": channel,
            "userId": user_id,
            "conversationId": conversation_id,
            "timestamp": timestamp,
            "content": {
                "type": 'location',
                "latitude": latitude,
                "longitude": longitude
            },
            "domain": domain,
            "intent": intent_dict,
            "entities": entity_list,
            "project": self._project,
            "language": language,
            "metadata": metadata,
            "type": "REQUEST"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_textual_request(self, message: str, message_id: str, user_id: str, channel: str,
                            timestamp: str, conversation_id: str = None, domain: str = None, intent_name: str = None,
                            intent_confidence: float = None,
                            entities: object = Optional[List[Entity]], language: object = None, metadata: object = Optional[dict]) -> str:
        """
        This method should be used to store a request message that contains text written by the user. A request message is a message from the user to the chatbot
        :param message: a string that indicates the text of the message
        :param message_id: the id of the message. This field should be a string
        :param user_id: the id of the user. This filed should be a string
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param timestamp: the timestamp of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param domain: this optional field is a string that describe the domain of the message
        :param intent_name: it is an optional string representing the name of the intent
        :param intent_confidence: it is an optional float number to measure the intent detection accuracy. If intent_name is defined, the default value of the confidence is 0.0
        :param entities: an optional list of entities. Each entity should have type Entity
        :param language: an optional string indicating the language of the message
        :param metadata: an optional dictionary containing additional information.
        :return:
        """
        if message_id is None or message_id == "":
            raise ValueError("message_id cannot be empty")

        if user_id is None or user_id == "":
            raise ValueError("user_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if timestamp is None or timestamp == "":
            raise ValueError("timestamp cannot be empty")

        if not isinstance(entities, list):
            entities = []

        if not isinstance(metadata, dict):
            metadata = {}

        api_point = self._access_point + "/messages"

        intent_dict = {
            'name': intent_name,
            'confidence': intent_confidence
        }

        temp_entities = []

        for entity in entities:
            temp_entities.append(entity.to_repr())

        message_generated = {
            "messageId": message_id,
            "channel": channel,
            "userId": user_id,
            "conversationId": conversation_id,
            "timestamp": timestamp,
            "content": {
                "type": 'text',
                "value": message
            },
            "domain": domain,
            "intent": intent_dict,
            "entities": temp_entities,
            "project": self._project,
            "language": language,
            "metadata": metadata,
            "type": "REQUEST"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_action_request(self, message: str, message_id: str, user_id: str, channel: str,
                           timestamp: str, conversation_id=None, domain=None, intent_name=None, intent_confidence=None,
                           entities=Optional[List[Entity]], language=None, metadata=Optional[dict]) -> str:
        """
        This method should be used to store a request message. This method should be used when the request message is generated by pressing a quick action button. A request message is a message from the user to the chatbot
        :param message: a string that indicates the text of the message
        :param message_id: the id of the message. This field should be a string
        :param user_id: the id of the user. This filed should be a string
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param timestamp: the timestamp of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param domain: this optional field is a string that describe the domain of the message
        :param intent_name: it is an optional string representing the name of the intent
        :param intent_confidence: it is an optional float number to measure the intent detection accuracy. If intent_name is defined, the default value of the confidence is 0.0
        :param entities: an optional list of entities. Each entity should have type Entity
        :param language: an optional string indicating the language of the message
        :param metadata: an optional dictionary containing additional information.
        :return:
        """
        if message_id is None or message_id == "":
            raise ValueError("message_id cannot be empty")

        if user_id is None or user_id == "":
            raise ValueError("user_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if timestamp is None or timestamp == "":
            raise ValueError("timestamp cannot be empty")

        if not isinstance(entities, list):
            entities = []

        if not isinstance(metadata, dict):
            metadata = {}

        api_point = self._access_point + "/messages"

        intent_dict = {
            'name': intent_name,
            'confidence': intent_confidence
        }

        temp_entities = []

        for entity in entities:
            if not isinstance(entity, Entity):
                raise ValueError("entities should be a list of Entity objects")
            else:
                temp_entities.append(entity.to_repr())

        message_generated = {
            "messageId": message_id,
            "channel": channel,
            "userId": user_id,
            "conversationId": conversation_id,
            "timestamp": timestamp,
            "content": {
                "type": 'action',
                "value": message
            },
            "domain": domain,
            "intent": intent_dict,
            "entities": temp_entities,
            "project": self._project,
            "language": language,
            "metadata": metadata,
            "type": "REQUEST"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_attachment_request(self, attachment_uri: str, message_id: str, user_id: str, channel: str, timestamp: str, conversation_id=None, alternative_text=None, domain=None, intent_name=None,
                               intent_confidence=None,
                               entities=Optional[List[Entity]], language=None, metadata=Optional[dict]) -> str:
        """
        This method should be used to store a request message that contains a file (e.g., an image) instead of text. A request message is a message from the user to the chatbot
        :param attachment_uri: the URI of the attached resource
        :param alternative_text: an alternative text to display whenever the preview of the file cannot be loaded and whenever accessibility tools are active.
        :param message_id: the id of the message. This field should be a string
        :param user_id: the id of the user. This filed should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param timestamp: the timestamp of the message. This field should be a string
        :param domain: this optional field is a string that describe the domain of the message
        :param intent_name: it is an optional string representing the name of the intent
        :param intent_confidence: it is an optional float number to measure the intent detection accuracy. If intent_name is defined, the default value of the confidence is 0.0
        :param entities: an optional list of entities. Each entity should have type Entity
        :param language: an optional string indicating the language of the message
        :param metadata: an optional dictionary containing additional information.
        :return:
        """
        if message_id is None or message_id == "":
            raise ValueError("message_id cannot be empty")

        if user_id is None or user_id == "":
            raise ValueError("user_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if timestamp is None or timestamp == "":
            raise ValueError("timestamp cannot be empty")

        if not isinstance(entities, list):
            entities = []

        if not isinstance(metadata, dict):
            metadata = {}

        api_point = self._access_point + "/messages"

        intent_dict = {
            'name': intent_name,
            'confidence': intent_confidence
        }

        temp_entities = []

        for entity in entities:
            temp_entities.append(entity.to_repr())

        message_generated = {
            "messageId": message_id,
            "channel": channel,
            "userId": user_id,
            "conversationId": conversation_id,
            "timestamp": timestamp,
            "content": {
                "type": 'attachment',
                "uri": attachment_uri,
                "alternativeText": alternative_text
            },
            "domain": domain,
            "intent": intent_dict,
            "entities": temp_entities,
            "project": self._project,
            "language": language,
            "metadata": metadata,
            "type": "REQUEST"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_textual_response(self, message_text: str, message_id: str, channel: str, user_id: str,
                             response_to: str, timestamp: str, conversation_id=None, buttons=Optional[List],
                             metadata=Optional[dict]) -> str:
        """
        This method should be used to store a response message. A response message is a message from the bot to the user that represents an answer to a request message
        :param message_text: the text of the message. It is a string
        :param message_id: the id of the message. This field should be a string
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param user_id: the id of the user. This filed should be a string
        :param response_to: the id of the request that generated this message. It is a string
        :param timestamp: the timestamp of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param buttons: a list of QuickReplyResponse objects. Each object represents a quick action button associated to the message. This field is optional
        :param metadata: an optional dictionary containing additional information.
        :return:
        """
        if message_text == "" or message_text is None:
            raise ValueError("message_text cannot be empty")

        if message_id == "" or message_id is None:
            raise ValueError("message_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if user_id == "" or user_id is None:
            raise ValueError("user_id cannot be empty")

        if response_to == "" or response_to is None:
            raise ValueError("response_to cannot be empty. If you are not answering to a specific message, please add a Notification instead of a Response")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp cannot be empty")

        if not isinstance(buttons, list):
            buttons = []

        if not isinstance(metadata, dict):
            metadata = {}

        api_point = self._access_point + "/messages"

        button_list = []
        for button in buttons:
            if button[1] == "" or button[1] is None:
                button_temp = ActionResponse(button[0])
            else:
                button_temp = ActionResponse(button[0], button_id=button[1])
            button_list.append(button_temp.to_repr())

        content_dict = {
            'type': 'text',
            'value': message_text,
            'buttons': button_list
        }

        message_generated = {
            "messageId": message_id,
            "conversationId": conversation_id,
            "channel": channel,
            "userId": user_id,
            "timestamp": timestamp,
            "responseTo": response_to,
            "content": content_dict,
            "metadata": metadata,
            "project": self._project,
            "type": "RESPONSE"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_attachment_response(self, attachment_uri: str, message_id: str, channel: str, user_id: str,
                                response_to: str, timestamp: str, conversation_id=None, alternative_text=None,
                                buttons=Optional[List], metadata=Optional[dict]) -> str:
        """
        This method should be used to store a response message. A response message is a message from the bot to the user that represents an answer to a request message
        :param attachment_uri: the uri of the resource associated to the message
        :param message_id: the id of the message. This field should be a string
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param user_id: the id of the user. This filed should be a string
        :param response_to: the id of the request that generated this message. It is a string
        :param timestamp: the timestamp of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param alternative_text: the alternative text of the media. It is a string and it is displayed whenever the media cannot be loaded correctly. Usually, it is a description of the media. This field is optional
        :param buttons: a list of QuickReply responses (list of buttons to let the user perform quick actions). This field is optional
        :param metadata: an optional dictionary containing additional information.
        :return:
        """

        if attachment_uri == "" or attachment_uri is None:
            raise ValueError("uri cannot be empty")

        if message_id == "" or message_id is None:
            raise ValueError("message_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if user_id == "" or user_id is None:
            raise ValueError("user_id cannot be empty")

        if response_to == "" or response_to is None:
            raise ValueError("response_to cannot be empty")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp cannot be empty")

        if not isinstance(buttons, list):
            buttons = []

        if not isinstance(metadata, dict):
            metadata = {}

        button_list = []
        for button in buttons:
            if button[1] == "" or button[1] is None:
                button_temp = ActionResponse(button[0])
            else:
                button_temp = ActionResponse(button[0], button_id=button[1])
            button_list.append(button_temp.to_repr())

        content_dict = {
            'type': 'attachment',
            'uri': attachment_uri,
            'alternativeText': alternative_text,
            'buttons': button_list
        }

        api_point = self._access_point + "/messages"

        message_generated = {
            "messageId": message_id,
            "conversationId": conversation_id,
            "channel": channel,
            "userId": user_id,
            "timestamp": timestamp,
            "responseTo": response_to,
            "content": content_dict,
            "metadata": metadata,
            "project": self._project,
            "type": "RESPONSE"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_quick_reply_response(self, buttons: list, message_id: str, channel: str, user_id: str,
                                 response_to: str, timestamp: str, conversation_id=None,
                                 metadata=Optional[dict]) -> str:
        """
        This method should be used to store a response message. A response message is a message from the bot to the user that represents an answer to a request message
        :param buttons: the list of the buttons
        :param message_id: the id of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param user_id: the id of the user. This filed should be a string
        :param response_to: the id of the request that generated this message. It is a string
        :param timestamp: the timestamp of the message. This field should be a string
        :param metadata: an optional dictionary containing additional information.
        :return:
        """

        if message_id == "" or message_id is None:
            raise ValueError("message_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if user_id == "" or user_id is None:
            raise ValueError("user_id cannot be empty")

        if response_to == "" or response_to is None:
            raise ValueError("response_to cannot be empty")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp cannot be empty")

        if not isinstance(buttons, list):
            buttons = []

        if not isinstance(metadata, dict):
            metadata = {}

        button_list = []
        for button in buttons:
            if button[1] == "" or button[1] is None:
                button_temp = ActionResponse(button[0])
            else:
                button_temp = ActionResponse(button[0], button_id=button[1])
            button_list.append(button_temp.to_repr())

        content_dict = {
            "type": "multiaction",
            "buttons": button_list
        }

        api_point = self._access_point + "/messages"

        message_generated = {
            "messageId": message_id,
            "conversationId": conversation_id,
            "channel": channel,
            "userId": user_id,
            "timestamp": timestamp,
            "responseTo": response_to,
            "content": content_dict,
            "metadata": metadata,
            "project": self._project,
            "type": "RESPONSE"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_carousel_response(self, carousel_items: list, message_id: str, channel: str, user_id: str,
                              response_to: str, timestamp: str, conversation_id=None,
                              metadata=Optional[dict]) -> str:
        """
        This method should be used to store a response message. A response message is a message from the bot to the user that represents an answer to a request message
        :param carousel_items: a list of CarouselItem objects
        :param message_id: the id of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param user_id: the id of the user. This filed should be a string
        :param response_to: the id of the request that generated this message. It is a string
        :param timestamp: the timestamp of the message. This field should be a string
        :param metadata: an optional dictionary containing additional information.
        :return:
        """

        if message_id == "" or message_id is None:
            raise ValueError("message_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if user_id == "" or user_id is None:
            raise ValueError("user_id cannot be empty")

        if response_to == "" or response_to is None:
            raise ValueError("response_to cannot be empty")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp cannot be empty")

        if not isinstance(metadata, dict):
            metadata = {}

        cards = []
        for item in carousel_items:
            if isinstance(item, CarouselCardResponse):
                cards.append(item.to_repr())

        content_dict = {
            'type': 'carousel',
            'cards': cards
        }

        api_point = self._access_point + "/messages"

        message_generated = {
            "messageId": message_id,
            "conversationId": conversation_id,
            "channel": channel,
            "userId": user_id,
            "timestamp": timestamp,
            "responseTo": response_to,
            "content": content_dict,
            "metadata": metadata,
            "project": self._project,
            "type": "NOTIFICATION"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_textual_notification(self, message_text: str, message_id: str, channel: str, user_id: str,
                                 timestamp: str, conversation_id=None, buttons=Optional[List],
                                 metadata=Optional[dict]) -> str:
        """
        This method should be used to store a notification message.
        :param message_text: the text of the message. It is a string
        :param message_id: the id of the message. This field should be a string
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param user_id: the id of the user. This filed should be a string
        :param timestamp: the timestamp of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param buttons: a list of QuickReplyResponse objects. Each object represents a quick action button associated to the message. This field is optional
        :param metadata: an optional dictionary containing additional information.
        :return:
        """
        if message_text == "" or message_text is None:
            raise ValueError("message_text cannot be empty")

        if message_id == "" or message_id is None:
            raise ValueError("message_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if user_id == "" or user_id is None:
            raise ValueError("user_id cannot be empty")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp cannot be empty")

        if not isinstance(buttons, list):
            buttons = []

        if not isinstance(metadata, dict):
            metadata = {}

        api_point = self._access_point + "/messages"

        button_list = []
        for button in buttons:
            if button[1] == "" or button[1] is None:
                button_temp = ActionResponse(button[0])
            else:
                button_temp = ActionResponse(button[0], button_id=button[1])
            button_list.append(button_temp.to_repr())

        content_dict = {
            'type': 'text',
            'value': message_text,
            'buttons': button_list
        }

        message_generated = {
            "messageId": message_id,
            "conversationId": conversation_id,
            "channel": channel,
            "userId": user_id,
            "timestamp": timestamp,
            "content": content_dict,
            "metadata": metadata,
            "project": self._project,
            "type": "NOTIFICATION"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_attachment_notification(self, attachment_uri: str, message_id: str, channel: str, user_id: str,
                                    timestamp: str, conversation_id=None, alternative_text=None,
                                    buttons=Optional[List], metadata=Optional[dict]) -> str:
        """
        This method should be used to store a notification message.
        :param attachment_uri: the uri of the resource associated to the message
        :param message_id: the id of the message. This field should be a string
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param user_id: the id of the user. This filed should be a string
        :param timestamp: the timestamp of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param alternative_text: the alternative text of the media. It is a string and it is displayed whenever the media cannot be loaded correctly. Usually, it is a description of the media. This field is optional
        :param buttons: a list of QuickReply responses (list of buttons to let the user perform quick actions). This field is optional
        :param metadata: an optional dictionary containing additional information.
        :return:
        """

        if attachment_uri == "" or attachment_uri is None:
            raise ValueError("uri cannot be empty")

        if message_id == "" or message_id is None:
            raise ValueError("message_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if user_id == "" or user_id is None:
            raise ValueError("user_id cannot be empty")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp cannot be empty")

        if not isinstance(buttons, list):
            buttons = []

        if not isinstance(metadata, dict):
            metadata = {}

        button_list = []
        for button in buttons:
            if button[1] == "" or button[1] is None:
                button_temp = ActionResponse(button[0])
            else:
                button_temp = ActionResponse(button[0], button_id=button[1])
            button_list.append(button_temp.to_repr())

        content_dict = {
            'type': 'attachment',
            'uri': attachment_uri,
            'alternativeText': alternative_text,
            'buttons': button_list
        }

        api_point = self._access_point + "/messages"

        message_generated = {
            "messageId": message_id,
            "conversationId": conversation_id,
            "channel": channel,
            "userId": user_id,
            "timestamp": timestamp,
            "content": content_dict,
            "metadata": metadata,
            "project": self._project,
            "type": "NOTIFICATION"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_quick_reply_notification(self, buttons: list, message_id: str, channel: str, user_id: str,
                                     timestamp: str, conversation_id=None,
                                     metadata=Optional[dict]) -> str:
        """
        This method should be used to store a notification message.
        :param buttons: the list of the buttons
        :param message_id: the id of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param user_id: the id of the user. This filed should be a string
        :param timestamp: the timestamp of the message. This field should be a string
        :param metadata: an optional dictionary containing additional information.
        :return:
        """

        if message_id == "" or message_id is None:
            raise ValueError("message_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if user_id == "" or user_id is None:
            raise ValueError("user_id cannot be empty")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp cannot be empty")

        if not isinstance(buttons, list):
            buttons = []

        if not isinstance(metadata, dict):
            metadata = {}

        button_list = []
        for button in buttons:
            if button[1] == "" or button[1] is None:
                button_temp = ActionResponse(button[0])
            else:
                button_temp = ActionResponse(button[0], button_id=button[1])
            button_list.append(button_temp.to_repr())

        content_dict = {
            "type": "multiaction",
            "buttons": button_list
        }

        api_point = self._access_point + "/messages"

        message_generated = {
            "messageId": message_id,
            "conversationId": conversation_id,
            "channel": channel,
            "userId": user_id,
            "timestamp": timestamp,
            "content": content_dict,
            "metadata": metadata,
            "project": self._project,
            "type": "NOTIFICATION"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def add_carousel_notification(self, carousel_items: list, message_id: str, channel: str, user_id: str,
                                  timestamp: str, conversation_id=None,
                                  metadata=Optional[dict]) -> str:
        """
        This method should be used to store a notification message.
        :param carousel_items: a list of CarouselItem objects
        :param message_id: the id of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param user_id: the id of the user. This filed should be a string
        :param timestamp: the timestamp of the message. This field should be a string
        :param metadata: an optional dictionary containing additional information.
        :return:
        """

        if message_id == "" or message_id is None:
            raise ValueError("message_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if user_id == "" or user_id is None:
            raise ValueError("user_id cannot be empty")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp cannot be empty")

        if not isinstance(metadata, dict):
            metadata = {}

        cards = []
        for item in carousel_items:
            if isinstance(item, CarouselCardResponse):
                cards.append(item.to_repr())

        content_dict = {
            'type': 'carousel',
            'cards': cards
        }

        api_point = self._access_point + "/messages"

        message_generated = {
            "messageId": message_id,
            "conversationId": conversation_id,
            "channel": channel,
            "userId": user_id,
            "timestamp": timestamp,
            "content": content_dict,
            "metadata": metadata,
            "project": self._project,
            "type": "NOTIFICATION"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=messages)

        if response.status_code == 201:
            dict_response = json.loads(response.text)
            return dict_response['traceIds'][0]
        else:
            raise ValueError("The message has not been logged")

    def get_message_from_message_id_and_user_id(self, message_id: str, user_id: str) -> dict:
        """
        Utils to retrieve a message from the database
        :param message_id: the if of the message to retrieve
        :param user_id: the if of the user related to message to retrieve
        :return: the status and the content of the response
        """
        api_point = self._access_point + "/message?project=" + self._project + "&messageId=" + message_id + "&userId=" + user_id

        response = requests.get(api_point, headers=self._custom_headers)

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise ValueError("Cannot retrieve the message")

    def get_message_from_trace_id(self, trace_id: str) -> dict:
        """
        Utils to retrieve a message from the database
        :param trace_id: the traceId of the message to retrieve
        :return: the status and the content of the response
        """
        api_point = self._access_point + "/message?traceId=" + trace_id

        response = requests.get(api_point, headers=self._custom_headers)

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise ValueError("Cannot retrieve the message")

    def get_messages(self, from_time: datetime, to_time: datetime, user_id: str = None, channel: str = None, message_type: str = None, max_size: int = 1000) -> list:
        """
        Utils to retrieve a message from the database
        :param from_time: the time from which retrieve the messages
        :param to_time:  the time up to which retrieve the messages
        :param user_id: the id of the user related to the messages to retrieve
        :param channel: the channel of the messages to retrieve
        :param message_type: the type of the messages to retrieve
        :param max_size: the maximum number of messages to retrieve (up to 10000)
        :return: the status and the content of the response
        """
        api_point = self._access_point + "/messages?project=" + self._project + "&fromTime=" + from_time.isoformat() + "&toTime=" + to_time.isoformat() + "&maxSize=" + str(max_size)

        if user_id:
            api_point = api_point + "&userId=" + user_id

        if channel:
            api_point = api_point + "&channel=" + channel

        if message_type:
            api_point = api_point + "&type=" + message_type

        response = requests.get(api_point, headers=self._custom_headers)

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise ValueError("Cannot retrieve the messages")

    def delete_message_from_message_id_and_user_id(self, message_id: str, user_id: str) -> dict:
        """
        Utils to delete a message from the database
        :param message_id: the if of the message to delete
        :param user_id: the if of the user related to message to delete
        :return: the status and the content of the response
        """
        api_point = self._access_point + "/message?project=" + self._project + "&messageId=" + message_id + "&userId=" + user_id

        response = requests.delete(api_point, headers=self._custom_headers)

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise ValueError("Cannot delete the message")

    def delete_message_from_trace_id(self, trace_id: str) -> dict:
        """
        Utils to delete a message from the database
        :param trace_id: the traceId of the message to delete
        :return: the status and the content of the response
        """
        api_point = self._access_point + "/message?traceId=" + trace_id

        response = requests.delete(api_point, headers=self._custom_headers)

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise ValueError("Cannot delete the message")

    def add_log(self, log_id: str, component: str, severity: str, log_content: str, timestamp: str, authority: str = None,  bot_version: str = None, metadata: dict = None) -> str:

        logger.info("Starting logging a new message ")

        if log_id == "" or log_id is None:
            raise ValueError("logId is missing and is required")

        if component == "" or component is None:
            raise ValueError("component is missing and is required")

        if severity == "" or severity is None:
            raise ValueError("severity is missing and is required")

        if log_content == "" or log_content is None:
            raise ValueError("logContent is missing and is required")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp is missing and is required")

        api_point = self._access_point + "/logs"

        log_generated = {
            "logId": log_id,
            "project": self._project,
            "component": component,
            "authority": authority,
            "severity": severity,
            "logContent": log_content,
            "timestamp": timestamp,
            "botVersion": bot_version,
            "metadata": metadata
        }

        logs = [log_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=logs)

        if response.status_code == 200:
            dict_response = json.loads(response.text)
            return dict_response['logId'][0]
        else:
            raise ValueError("The log has not been logged")

    def get_analytic_result(self, analytic: CommonAnalyticDescriptor, sleep_time: int = 1, number_of_trials: int = 10) -> Optional[dict]:

        json_payload = analytic.to_repr()

        api_point = self._access_point + "/analytic"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers={**headers, **self._custom_headers}, json=json_payload)

        analytic_id = json.loads(response.content)["id"]

        for i in range(number_of_trials):
            sleep(sleep_time)
            requests.post(api_point + "/compute", headers=self._custom_headers, params={"id": analytic_id})
            sleep(sleep_time)
            response = requests.get(api_point, headers=self._custom_headers, params={"id": analytic_id})
            if response.status_code == 200 and json.loads(response.content)["result"] is not None:
                break

        requests.delete(api_point, headers={**headers, **self._custom_headers}, params={"id": analytic_id})

        if response.status_code == 200:
            return json.loads(response.content)["result"]
        else:
            raise Exception(f"request has return a code {response.status_code} with content {response.content}")
