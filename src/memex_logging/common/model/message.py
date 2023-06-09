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

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Optional, List

logger = logging.getLogger("logger.common.model.message")


class Intent:

    def __init__(self, name: Optional[str], confidence: Optional[float] = None) -> None:
        """
        The intent expressed by a user.

        :param Optional[str] name: the label identifying the intent
        :param Optional[float] confidence: the confidence
        """
        self.name: Optional[str] = name
        self.confidence: Optional[str] = confidence

    def to_repr(self) -> dict:
        return {
            'name': self.name,
            'confidence': self.confidence
        }

    @staticmethod
    def from_repr(data: dict) -> Intent:
        name = data.get("name", None)
        confidence = data.get("confidence", None)
        return Intent(name, confidence)

    def __eq__(self, o: Intent) -> bool:
        return isinstance(o, Intent) and o.name == self.name and o.confidence == self.confidence


class UserInfoRequest:

    def __init__(self, info_type: str, value: str) -> None:
        self.type = info_type
        self.value = value

    def to_repr(self) -> dict:
        return {
            'type': self.type,
            'value': self.value
        }

    @staticmethod
    def from_repr(data: dict) -> UserInfoRequest:
        return UserInfoRequest(data['type'], data['value'])

    def __eq__(self, o: UserInfoRequest) -> bool:
        return isinstance(o, UserInfoRequest) and o.type == self.type and o.value == self.value


class LocationRequest:

    def __init__(self, latitude: float, longitude: float) -> None:
        self.latitude = latitude
        self.longitude = longitude

    def to_repr(self) -> dict:
        return {
            'type': 'location',
            'latitude': self.latitude,
            'longitude': self.longitude
        }

    @staticmethod
    def from_repr(data: dict) -> LocationRequest:
        return LocationRequest(data['latitude'], data['longitude'])

    def __eq__(self, o: LocationRequest) -> bool:
        return isinstance(o, LocationRequest) and o.latitude == self.latitude and o.longitude == self.longitude


class LocationResponse:

    def __init__(self, latitude: float, longitude: float, buttons: Optional[List[ActionResponse]]) -> None:
        self.latitude = latitude
        self.longitude = longitude
        self.buttons = buttons

    def to_repr(self) -> dict:
        buttons = []
        for button in self.buttons:
            if not isinstance(button, ActionResponse):
                raise ValueError("The elements in the button list are not instances of ActionResponse")
            else:
                buttons.append(button.to_repr())

        return {
            'type': 'location',
            'latitude': self.latitude,
            'longitude': self.longitude,
            'buttons': buttons
        }

    @staticmethod
    def from_repr(data: dict) -> LocationResponse:
        buttons = []
        if 'buttons' in data:
            for action in data['buttons']:
                a = ActionResponse.from_repr(action)
                buttons.append(a)

        return LocationResponse(data['latitude'], data['longitude'], buttons)

    def __eq__(self, o: LocationResponse) -> bool:
        return isinstance(o, LocationResponse) and o.latitude == self.latitude and o.longitude == self.longitude and o.buttons == self.buttons


class ActionRequest:

    def __init__(self, value: str) -> None:
        self.value: str = value

    def to_repr(self) -> dict:
        return {
            'type': 'action',
            'value': self.value
        }

    @staticmethod
    def from_repr(data: dict) -> ActionRequest:
        return ActionRequest(
            data.get("value")
        )

    def __eq__(self, o: ActionRequest) -> bool:
        return isinstance(o, ActionRequest) and o.value == self.value


class ActionResponse:

    def __init__(self, button_text: str, button_id: Optional[str] = None) -> None:
        """
        Create an ActionResponse object. An action response is a button that suggest the user an action to perform.
        :param button_text: the text of the button
        :param button_id: optionally, the id of the button
        """

        self.button_text = button_text
        self.button_id = button_id

    def to_repr(self) -> dict:
        return {
            'type': 'action',
            'buttonText': self.button_text,
            'buttonId': self.button_id
        }

    @staticmethod
    def from_repr(data: dict) -> ActionResponse:
        button_id = None
        if "buttonId" in data:
            button_id = data['buttonId']

        return ActionResponse(data['buttonText'], button_id)

    def __eq__(self, o: ActionResponse) -> bool:
        return isinstance(o, ActionResponse) and o.button_text == self.button_text and o.button_id == self.button_id


class MultiActionResponse:

    def __init__(self, buttons: Optional[List[ActionResponse]]) -> None:
        self.buttons = buttons

    def to_repr(self) -> dict:
        buttons = []
        for button in self.buttons:
            if not isinstance(button, ActionResponse):
                raise ValueError("The elements in the button list are not instances of ActionResponse")
            else:
                buttons.append(button.to_repr())

        return {
            'type': 'multiaction',
            'buttons': buttons
        }

    @staticmethod
    def from_repr(data: dict) -> MultiActionResponse:
        buttons = []
        if 'buttons' in data:
            for action in data['buttons']:
                a = ActionResponse.from_repr(action)
                buttons.append(a)

        return MultiActionResponse(buttons)

    def __eq__(self, o: MultiActionResponse) -> bool:
        return isinstance(o, MultiActionResponse) and o.buttons == self.buttons


class TextualRequest:

    def __init__(self, value: str) -> None:
        self.value = value

    def to_repr(self) -> dict:
        return {
            'type': 'text',
            'value': self.value
        }

    @staticmethod
    def from_repr(data: dict) -> TextualRequest:
        return TextualRequest(data['value'])

    def __eq__(self, o: TextualRequest) -> bool:
        return isinstance(o, TextualRequest) and o.value == self.value


class TextualResponse:

    def __init__(self, value: str, buttons: Optional[List[ActionResponse]]) -> None:
        self.value = value
        self.buttons = buttons

    def to_repr(self) -> dict:
        buttons = []
        for button in self.buttons:
            if not isinstance(button, ActionResponse):
                raise ValueError("The elements in the button list are not instances of ActionResponse")
            else:
                buttons.append(button.to_repr())

        return {
            'type': 'text',
            'value': self.value,
            'buttons': buttons
        }

    @staticmethod
    def from_repr(data: dict) -> TextualResponse:
        buttons = []
        if 'buttons' in data:
            for action in data['buttons']:
                a = ActionResponse.from_repr(action)
                buttons.append(a)

        return TextualResponse(data['value'], buttons)

    def __eq__(self, o: TextualResponse) -> bool:
        return isinstance(o, TextualResponse) and o.value == self.value and o.buttons == self.buttons


class AttachmentRequest:

    def __init__(self, uri: str, alternative_text: Optional[str]) -> None:
        self.uri = uri
        self.alternative_text = alternative_text

    def to_repr(self) -> dict:
        return {
            "type": "attachment",
            "uri": self.uri,
            "alternativeText": self.alternative_text
        }

    @staticmethod
    def from_repr(data: dict) -> AttachmentRequest:
        alt = None
        if 'alternativeText' in data:
            alt = data['alternativeText']

        return AttachmentRequest(data['uri'], alt)

    def __eq__(self, o: AttachmentRequest) -> bool:
        return isinstance(o, AttachmentRequest) and o.uri == self.uri and o.alternative_text == self.alternative_text


class AttachmentResponse:

    def __init__(self, uri: str, alternative_text: Optional[str], buttons: Optional[List[ActionResponse]]) -> None:
        """
        Create an AttachmentResponse Object. An attachment response is a response containing only a media
        :param uri: uri of the media as a string
        :param alternative_text: the alternative text of the media. It is a string and it is displayed whenever the media cannot be loaded correctly. Usually, it is a description of the media
        :param buttons: a list of ActionResponse responses (list of buttons to let the user perform actions). This field is optional
        """
        self.uri = uri
        self.alternative_text = alternative_text
        self.buttons = buttons

    def to_repr(self) -> dict:
        buttons = []
        for button in self.buttons:
            if not isinstance(button, ActionResponse):
                raise ValueError("The elements in the button list are not instances of ActionResponse")
            else:
                buttons.append(button.to_repr())

        return {
            "type": "attachment",
            "uri": self.uri,
            "alternativeText": self.alternative_text,
            "buttons": buttons
        }

    @staticmethod
    def from_repr(data: dict) -> AttachmentResponse:
        buttons = []
        if 'buttons' in data:
            for action in data['buttons']:
                a = ActionResponse.from_repr(action)
                buttons.append(a)

        alt = None
        if 'alternativeText' in data:
            alt = data['alternativeText']

        return AttachmentResponse(data['uri'], alt, buttons)

    def __eq__(self, o: AttachmentResponse) -> bool:
        return isinstance(o, AttachmentResponse) and o.uri == self.uri and o.alternative_text == self.alternative_text and o.buttons == self.buttons


class CarouselCardResponse:

    def __init__(self, title: str, image_url: Optional[str], subtitle: Optional[str], default_action: Optional[dict], buttons: Optional[List[ActionResponse]]) -> None:
        """
        Create a CarouselCardResponse Object. A carousel is a scrollable list of medias. As a best-practice, carousels should be used with no more that 6 elements and when the elements can be ranked.
        :param title: the title of the slide of the carousel
        :param image_url: the url of the media
        :param subtitle: eventually, a subtitle for the slide
        :param default_action: the default action the user may take. It is a dictionary and it is optional
        :param buttons: a list of ActionResponse responses (list of buttons to let the user perform actions)
        """

        self.title = title
        self.image_url = image_url
        self.subtitle = subtitle
        self.default_action = default_action
        self.buttons = buttons

    def to_repr(self) -> dict:
        buttons = []
        for button in self.buttons:
            if not isinstance(button, ActionResponse):
                raise ValueError("The elements in the button list are not instances of ActionResponse")
            else:
                buttons.append(button.to_repr())

        return {
            'title': self.title,
            'imageUrl': self.image_url,
            'subtitle': self.subtitle,
            'defaultAction': self.default_action,
            'buttons': buttons
        }

    @staticmethod
    def from_repr(data: dict) -> CarouselCardResponse:
        buttons = []
        if 'buttons' in data:
            for action in data['buttons']:
                a = ActionResponse.from_repr(action)
                buttons.append(a)

        image_url = None
        if 'imageUrl' in data:
            image_url = data['imageUrl']

        subtitle = None
        if 'subtitle' in data:
            subtitle = data['subtitle']

        default_action = None
        if 'defaultAction' in data:
            default_action = data['defaultAction']

        return CarouselCardResponse(data['title'], image_url, subtitle, default_action, buttons)

    def __eq__(self, o: CarouselCardResponse) -> bool:
        return isinstance(o, CarouselCardResponse) and o.title == self.title and o.image_url == self.image_url and o.subtitle == self.subtitle and o.default_action == self.default_action and o.buttons == self.buttons


class CarouselResponse:

    def __init__(self, cards: List[CarouselCardResponse]) -> None:
        self.cards = cards

    def to_repr(self) -> dict:
        cards = []
        for card in self.cards:
            if not isinstance(card, CarouselCardResponse):
                raise ValueError("Each card are not instances of CarouselCardResponse")
            else:
                cards.append(card.to_repr())

        return {
            'type': 'carousel',
            'cards': cards
        }

    @staticmethod
    def from_repr(data: dict) -> CarouselResponse:
        cards = []
        for card in data['cards']:
            c = CarouselCardResponse.from_repr(card)
            cards.append(c)

        return CarouselResponse(cards)

    def __eq__(self, o: CarouselResponse) -> bool:
        return isinstance(o, CarouselResponse) and o.cards == self.cards


class Entity:

    def __init__(self, entity_type: str, value: str, confidence: float) -> None:
        """
        Method to create an Entity
        :param entity_type: the type of the entity (i.e., @city)
        :param value: the value of the entity (i.e., Boston)
        :param confidence: the confidence with which the entity has been detected
        """

        self.type = entity_type
        self.value = value
        self.confidence = confidence

    def to_repr(self) -> dict:
        return {
            'type': self.type,
            'value': self.value,
            'confidence': self.confidence
        }

    @staticmethod
    def from_repr(data: dict) -> Entity:
        return Entity(data['type'], data['value'], data['confidence'])

    def __eq__(self, o: Entity) -> bool:
        return isinstance(o, Entity) and o.type == self.type and o.value == self.value and o.confidence == self.confidence


class MessageType(Enum):

    REQUEST = "request"
    RESPONSE = "response"
    NOTIFICATION = "notification"


class Message(ABC):
    """
    A generic message that can be send within a conversation.
    This can and should be used in one of its extended forms:
      - RequestMessage
      - ResponseMessage
      - NotificationMessage
    """

    def __init__(self, message_id: str, conversation_id: Optional[str], channel: str, user_id: str, timestamp: datetime,
                 content, metadata: Optional[dict], project: str) -> None:
        """
        :param str message_id: the message id
        :param Optional[str] conversation_id: the conversation identifier
        :param str channel: the channel where the message is sent
        :param str user_id: the identifier of the user sending the message
        :param datetime timestamp: the datetime of the instant the message was sent
        :param content: the content of the message
        :param Optional[dict] metadata: any metadata (key/value) associated to the message
        :param str project: the project associated to the conversation
        """

        self.message_id = message_id
        self.conversation_id = conversation_id
        self.channel = channel
        self.user_id = user_id
        self.timestamp = timestamp
        self.content = content
        self.metadata = metadata
        self.project = project

        if metadata is not None:
            if not isinstance(metadata, dict):
                raise ValueError("Parameter `metadata` is not a dict")

    @staticmethod
    def from_repr(raw_data: dict) -> Message:
        message_type = raw_data['type'].lower()
        if message_type == MessageType.REQUEST.value:
            return RequestMessage.from_repr(raw_data)
        elif message_type == MessageType.RESPONSE.value:
            return ResponseMessage.from_repr(raw_data)
        elif message_type == MessageType.NOTIFICATION.value:
            return NotificationMessage.from_repr(raw_data)
        else:
            logger.error(f"Not supported message type {message_type}")
            raise ValueError(f"Unable to build a Message of type [{message_type}]")

    @staticmethod
    def timestamp_str_to_datetime(timestamp_str: str) -> datetime:
        return datetime.fromisoformat(timestamp_str)

    @abstractmethod
    def to_repr(self) -> dict:
        pass


class RequestMessage(Message):

    ALLOWED_CONTENT_TYPES = [
        TextualRequest,
        ActionRequest,
        AttachmentRequest,
        LocationRequest,
        UserInfoRequest
    ]

    def __init__(self, message_id: str, conversation_id: Optional[str], channel: str, user_id: str, timestamp: datetime,
                 content, domain: Optional[str], intent: Optional[Intent], entities: List[Entity], language: Optional[str],
                 metadata: Optional[dict], project: str) -> None:

        super().__init__(message_id, conversation_id, channel, user_id, timestamp, content, metadata, project)

        self.domain = domain
        self.intent = intent
        self.entities = entities
        self.language = language

        if intent is not None:
            if not isinstance(intent, Intent):
                raise ValueError("Parameter `intent` is not a Intent")

        if content is not None:
            if type(content) not in self.ALLOWED_CONTENT_TYPES:
                raise ValueError(f"Type for parameter `content` is not allowed - {type(content)}")

        if not isinstance(entities, list):
            raise ValueError("Parameter `entities` is not a list")
        else:
            for entity in entities:
                if not isinstance(entity, Entity):
                    raise ValueError("Parameter `entities` should contain only Entity objects")

    def to_repr(self) -> dict:
        entities = []
        for entity in self.entities:
            entities.append(entity.to_repr())

        local_content = None
        if self.content is not None:
            local_content = self.content.to_repr()

        local_intent = None
        if self.intent is not None:
            local_intent = self.intent.to_repr()

        return {
            'messageId': self.message_id,
            'conversationId': self.conversation_id,
            'channel': self.channel,
            'userId': self.user_id,
            'timestamp': self.timestamp.isoformat(),
            'content': local_content,
            'domain': self.domain,
            'intent': local_intent,
            'entities': entities,
            'language': self.language,
            'metadata': self.metadata,
            'project': self.project,
            'type': MessageType.REQUEST.value
        }

    @staticmethod
    def from_repr(raw_data: dict) -> RequestMessage:
        intent = raw_data.get("intent", None)
        if intent:
            intent = Intent.from_repr(intent)

        content = raw_data.get("content", None)
        user_info_fields = ['firstName', 'lastName', 'profilePic', 'locale', 'timezone', 'gender', 'isPaymentEnable', 'userId']
        if content:
            content_type = content['type'].lower()
            if content_type == "text":
                content = TextualRequest.from_repr(content)
            elif content_type == "action":
                content = ActionRequest.from_repr(content)
            elif content_type == "attachment":
                content = AttachmentRequest.from_repr(content)
            elif content_type == "location":
                content = LocationRequest.from_repr(content)
            elif content_type in user_info_fields:
                content = UserInfoRequest.from_repr(content)
            else:
                raise ValueError(f"Unsupported content type {content['type']} was provided")

        raw_entities = raw_data.get("entities", [])
        entities = [Entity.from_repr(raw_entity) for raw_entity in raw_entities]

        return RequestMessage(
            raw_data['messageId'],
            raw_data.get("conversationId", None),
            raw_data['channel'],
            raw_data['userId'],
            Message.timestamp_str_to_datetime(raw_data['timestamp']),
            content,
            raw_data.get("domain", None),
            intent,
            entities,
            raw_data.get("language", None),
            raw_data.get("metadata", None),
            raw_data['project']
        )


class ResponseMessage(Message):

    ALLOWED_CONTENT_TYPES = [
        MultiActionResponse,
        CarouselResponse,
        AttachmentResponse,
        TextualResponse,
        LocationResponse
    ]

    def __init__(self, message_id: str, conversation_id: Optional[str], channel: str, user_id: str, response_to: str,
                 timestamp: datetime, content, metadata: Optional[dict], project: str) -> None:

        super().__init__(message_id, conversation_id, channel, user_id, timestamp, content, metadata, project)

        self.response_to = response_to

        if content is not None:
            if type(content) not in self.ALLOWED_CONTENT_TYPES:
                raise ValueError(f"Type for parameter `content` is not allowed - {type(content)}")

    def to_repr(self) -> dict:
        local_content = None
        if self.content is not None:
            local_content = self.content.to_repr()

        return {
            'messageId': self.message_id,
            'conversationId': self.conversation_id,
            'channel': self.channel,
            'userId': self.user_id,
            'responseTo': self.response_to,
            'timestamp': self.timestamp.isoformat(),
            'content': local_content,
            'metadata': self.metadata,
            'project': self.project,
            'type': MessageType.RESPONSE.value
        }

    @staticmethod
    def from_repr(raw_data: dict) -> ResponseMessage:
        content = raw_data.get("content", None)
        if content:
            content_type = content['type'].lower()
            if content_type == "text":
                content = TextualResponse.from_repr(content)
            elif content_type == "location":
                content = LocationResponse.from_repr(content)
            elif content_type == "multiaction":
                content = MultiActionResponse.from_repr(content)
            elif content_type == "attachment":
                content = AttachmentResponse.from_repr(content)
            elif content_type == "carousel":
                content = CarouselResponse.from_repr(content)
            else:
                raise ValueError(f"Unsupported content type {content_type} was provided")

        return ResponseMessage(
            raw_data['messageId'],
            raw_data.get("conversationId", None),
            raw_data['channel'],
            raw_data['userId'],
            raw_data['responseTo'],
            Message.timestamp_str_to_datetime(raw_data['timestamp']),
            content,
            raw_data.get("metadata", None),
            raw_data['project']
        )


class NotificationMessage(Message):

    ALLOWED_CONTENT_TYPES = [
        MultiActionResponse,
        CarouselResponse,
        AttachmentResponse,
        TextualResponse,
        LocationResponse
    ]

    def __init__(self, message_id: str, conversation_id: Optional[str], channel: str, user_id: str, timestamp: datetime,
                 content, metadata: Optional[dict], project: str) -> None:

        super().__init__(message_id, conversation_id, channel, user_id, timestamp, content, metadata, project)

        if content is not None:
            if type(content) not in self.ALLOWED_CONTENT_TYPES:
                raise ValueError(f"Type for parameter `content` is not allowed - {type(content)}")

    def to_repr(self) -> dict:
        local_content = None

        if self.content is not None:
            local_content = self.content.to_repr()

        return {
            'messageId': self.message_id,
            'conversationId': self.conversation_id,
            'channel': self.channel,
            'userId': self.user_id,
            'timestamp': self.timestamp.isoformat(),
            'content': local_content,
            'metadata': self.metadata,
            'project': self.project,
            'type': MessageType.NOTIFICATION.value
        }

    @staticmethod
    def from_repr(raw_data: dict) -> NotificationMessage:
        content = raw_data.get("content", None)
        if content:
            content_type = content['type'].lower()
            if content_type == "text":
                content = TextualResponse.from_repr(content)
            elif content_type == "location":
                content = LocationResponse.from_repr(content)
            elif content_type == "multiaction":
                content = MultiActionResponse.from_repr(content)
            elif content_type == "attachment":
                content = AttachmentResponse.from_repr(content)
            elif content_type == "carousel":
                content = CarouselResponse.from_repr(content)
            else:
                raise ValueError(f"Unsupported content type {content_type} was provided")

        return NotificationMessage(
            raw_data['messageId'],
            raw_data.get("conversationId", None),
            raw_data['channel'],
            raw_data['userId'],
            Message.timestamp_str_to_datetime(raw_data['timestamp']),
            content,
            raw_data.get("metadata", None),
            raw_data['project']
        )
