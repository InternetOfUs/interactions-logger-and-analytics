from __future__ import absolute_import, annotations

from typing import Optional, List

import logging


class Intent:
    def __init__(self, name: str, confidence: float) -> None:
        self.name = name
        self.confidence = confidence

    def to_repr(self) -> dict:
        return {
            'name': self.name,
            'confidence': self.confidence
        }

    @staticmethod
    def from_rep(data: dict) -> Intent:
        name = None

        if 'name' in data:
            name = data['name']

        confidence = None

        if 'confidence' in data:
            confidence = data['confidence']

        return Intent(name,confidence)


class LocationRequest:
    def __init__(self, latitude: float, longitude: float) -> None:
        self.latitude = latitude
        self.longitude = longitude

    def to_repr(self) -> dict:
        return{
            'type': 'location',
            'latitude': self.latitude,
            'longitude': self.longitude
        }

    @staticmethod
    def from_rep(data: dict)-> LocationRequest:
        if 'latitude' not in data:
            raise ValueError("A LocationRequest object must contain a latitude and a longitude. Latitude is missing")

        if 'longitude' not in data:
            raise ValueError("A LocationRequest object must contain a latitude and a longitude. Longitude is missing")

        return LocationRequest(data['latitude'], data['longitude'])


class TextualRequest:

    def __init__(self, value):
        self.value = value

    def to_repr(self) -> dict:
        return {
            'type': 'text',
            'value': self.value
        }

    @staticmethod
    def from_rep(data: dict) -> TextualRequest:
        if 'value' in data:
            return TextualRequest(data['value'])
        else:
            raise ValueError("A TextualRequest object must contain a value")


class ActionRequest:

    def __init__(self, value):
        self.value = value

    def to_repr(self) -> dict:
        return {
            'type': 'action',
            'value': self.value
        }

    @staticmethod
    def from_rep(data: dict) -> ActionRequest:
        if 'value' in data:
            return ActionRequest(data['value'])
        else:
            raise ValueError("An ActionRequest object must contain a value")


class AttachmentRequest:
    def __init__(self, uri: str, alternative_text: str):
        self.uri = uri
        self.alternative_text = alternative_text

    def to_repr(self) -> dict:
        return {
            "type": "attachment",
            "uri": self.uri,
            "alternativeText": self.alternative_text
        }

    @staticmethod
    def from_rep(data: dict)-> AttachmentRequest:
        alt = None
        if 'alternativeText' in data:
            alt = data['alternativeText']
        if 'uri' not in data:
            raise ValueError("An AttachmentRequest object must contain an uri")
        return AttachmentRequest(data['uri'], alt)


class Entity:
    def __init__(self, type: str, value: str, confidence: float):
        self.type = type
        self.value = value
        self.confidence = confidence

    def to_repr(self) -> dict:
        return {
            'type': self.type,
            'value': self.value,
            'confidence': self.confidence
        }

    @staticmethod
    def from_rep(data: dict) -> Entity:
        if 'type' not in data:
            raise ValueError("An Entity must contain a type")
        if 'value' not in data:
            raise ValueError("An Entity must contain a value")
        if 'confidence' not in data:
            raise ValueError("An Entity must contain a confidence")
        return Entity(data['type'], data['value'], data['confidence'])


class RequestMessage:

    def __init__(self, message_id: str, channel: str, user_id: str, conversation_id: str, timestamp: str, content, domain: str,  intent: Intent, entities: list, project: str,
                 language: str, metadata: dict) -> None:
        logging.info("MODELS.MESSAGE creating a RequestMessage object for ".format(message_id))

        self.message_id = message_id
        self.channel = channel
        self.user_id = user_id
        self.conversation_id = conversation_id
        self.timestamp = timestamp
        self.content = content
        self.domain = domain
        self.intent = intent
        self.entities = entities
        self.project = project
        self.language = language
        self.metadata = metadata

        if not isinstance(intent, Intent):
            raise ValueError('intent type should be Intent')

        logging.info("MODELS.MESSAGE  intent check passed for ".format(message_id))

        if metadata is not None:
            if not isinstance(metadata, dict):
                raise ValueError('metadata type should be dictionary')

        logging.info("MODELS.MESSAGE  metadata check passed for ".format(message_id))

        if content is not None:
            if not (isinstance(content, TextualRequest) or isinstance(content, ActionRequest) or isinstance(content, AttachmentRequest) or isinstance(content, LocationRequest)):
                raise ValueError("content should be TextRequest, ActionRequest, AttachmentRequest or LocationRequest")

        logging.info("MODELS.MESSAGE  content check passed for ".format(message_id))

        for entity in entities:
            if not isinstance(entity, Entity):
                raise ValueError('entities should contains only object with type Entity')

        logging.info("MODELS.MESSAGE  entity check passed for ".format(message_id))

    def to_repr(self) -> dict:
        entities = []
        for entity in self.entities:
            entities.append(entity.to_repr)

        local_content = None

        if self.content is not None:
            local_content = self.content.to_repr()

        local_intent = None

        if self.intent is not None:
            local_intent = self.intent.to_repr()

        return {
            'messageId': self.message_id,
            'channel': self.channel,
            'userId': self.user_id,
            'conversationId': self.conversation_id,
            'timestamp': self.timestamp,
            'content': local_content,
            'domain': self.domain,
            'intent': local_intent,
            'entities': entities,
            'project': self.project,
            'language': self.language,
            'metadata': self.metadata,
            'type': 'REQUEST'
        }

    @staticmethod
    def from_rep(data: dict):

        logging.info("MODELS.MESSAGE starting logging a REQUEST message{}".format(data['messageId']))

        if 'intent' in data:
            intent = Intent.from_rep(data['intent'])
        else:
            intent_value = {
                'name': None,
                'confidence': 0.0
            }
            intent = Intent.from_rep(intent_value)

        if 'domain' in data:
            domain = data['domain']
        else:
            domain = None

        if 'conversationId' in data:
            conversation_id = data['conversationId']
        else:
            conversation_id = None

        content = None

        if "content" in data:
            if data['content']['type'] == "text":
                content = TextualRequest.from_rep(data['content'])
            elif data['content']['type'] == "action":
                content = ActionRequest.from_rep(data['content'])
            elif data['content']['type'] == "attachment":
                content = AttachmentRequest.from_rep(data['content'])
            elif data['content']['type'] == "location":
                content = LocationRequest.from_rep(data['content'])

        entities = []
        if "entities" in data:
            for entity in data['entities']:
                e = Entity.from_rep(entity)
                entities.append(e)

        metadata = None
        if "metadata" in data:
            metadata = data['metadata']

        language = None
        if "language" in data:
            language = data['language']

        logging.warning("MODELS.MESSAGE default parameters set up for ".format(data['messageId']))

        return RequestMessage(data['messageId'], data['channel'], data['userId'], conversation_id, data['timestamp'], content, domain, intent, entities,
                              data['project'], language, metadata)


class ResponseMessage:
    def __init__(self, message_id: str, conversation_id: str, channel: str, user_id: str, response_to: str, timestamp: str, content: list, metadata: dict, project: str):

        logging.info("MODELS.MESSAGE creating a ResponseMessage object for ".format(message_id))

        self.message_id = message_id
        self.conversation_id = conversation_id
        self.channel = channel
        self.user_id = user_id
        self.response_to = response_to
        self.timestamp = timestamp
        self.content = content
        self.metadata = metadata
        self.project = project
        self.type = "RESPONSE"

        for cont in content:
            if not (isinstance(cont, QuickReplyResponse) or isinstance(cont, CarouselResponse) or isinstance(cont, AttachmentResponse) or isinstance(cont, TextualRequest) or isinstance(cont, LocationRequest)):
                raise ValueError("response should contains only elements from QuickReplyResponse, CarouselResponse, AttachmentResponse, TextResponse")

        logging.info("MODELS.MESSAGE  content check passed for ".format(message_id))

        if metadata is not None:
            if not isinstance(metadata, dict):
                raise ValueError('metadata type should be dictionary')

        logging.info("MODELS.MESSAGE  metadata check passed for ".format(message_id))

    def to_repr(self) -> dict:
        content_list = []
        for c in self.content:
            content_list.append(c.to_repr())

        return {
            'messageId': self.message_id,
            'conversationId': self.conversation_id,
            'channel': self.channel,
            'userId': self.user_id,
            'responseTo': self.response_to,
            'timestamp': self.timestamp,
            'content': content_list,
            'metadata': self.metadata,
            'project': self.project,
            'type': self.type
        }

    @staticmethod
    def from_rep(data: dict):

        logging.info("MODELS.MESSAGE starting logging a RESPONSE message{}".format(data['messageId']))

        metadata = None
        if "metadata" in data:
            metadata = data["metadata"]

        content = []
        if 'content' in data:
            for item in data['content']:
                if item['type'] == 'text':
                    element = TextualRequest.from_rep(item)
                    content.append(element)
                elif item['type'] == 'location':
                    element = LocationRequest.from_rep(item)
                    content.append(element)
                elif item['type'] == 'action':
                    element = QuickReplyResponse.from_rep(item)
                    content.append(element)
                elif item['type'] == 'attachment':
                    element = AttachmentResponse.from_rep(item)
                    content.append(element)
                elif item['type'] == 'carousel':
                    element = CarouselResponse.from_rep(item)
                    content.append(element)

        if 'conversationId' in data:
            conversation_id = data['conversationId']
        else:
            conversation_id = None

        logging.warning("MODELS.MESSAGE default parameters set up for ".format(data['messageId']))

        return ResponseMessage(data['messageId'], conversation_id, data['channel'], data['userId'], data['responseTo'], data['timestamp'], content, metadata, data['project'])


class NotificationMessage:
    def __init__(self, message_id: str, conversation_id: str, channel: str, user_id: str, timestamp: str, content: list, metadata: dict, project: str):

        logging.info("MODELS.MESSAGE creating a NotificationMessage object for ".format(message_id))

        self.message_id = message_id
        self.conversation_id = conversation_id
        self.channel = channel
        self.user_id = user_id
        self.timestamp = timestamp
        self.content = content
        self.metadata = metadata
        self.project = project
        self.type = type

        for cont in content:
            if not (isinstance(cont, QuickReplyResponse) or isinstance(cont, CarouselResponse) or isinstance(cont, AttachmentResponse) or isinstance(cont, TextualRequest) or isinstance(cont, LocationRequest)):
                raise ValueError("response should contains only elements from QuickReplyResponse, CarouselResponse, AttachmentResponse, TextResponse")

        logging.info("MODELS.MESSAGE  content check passed for ".format(message_id))

        if metadata is not None:
            if not isinstance(metadata, dict):
                raise ValueError('metadata type should be dictionary')

        logging.info("MODELS.MESSAGE  metadata check passed for ".format(message_id))

    def to_repr(self) -> dict:
        content_list = []
        for c in self.content:
            content_list.append(c.to_repr)

        return {
            'messageId': self.message_id,
            'conversationId': self.conversation_id,
            'channel': self.channel,
            'userId': self.user_id,
            'timestamp': self.timestamp,
            'content': content_list,
            'metadata': self.metadata,
            'project': self.project,
            'type': self.type
        }

    @staticmethod
    def from_rep(data: dict):

        logging.info("MODELS.MESSAGE starting logging a NOTIFICATION message{}".format(data['messageId']))

        metadata = None
        if "metadata" in data:
            metadata = data["metadata"]

        content = []
        if 'content' in data:
            for item in data['content']:
                if item['type'] == 'text':
                    element = TextualRequest.from_rep(item)
                    content.append(element)
                elif item['type'] == 'action':
                    element = QuickReplyResponse.from_rep(item)
                    content.append(element)
                elif item['type'] == 'attachment':
                    element = AttachmentResponse.from_rep(item)
                    content.append(element)
                elif item['type'] == 'carousel':
                    element = CarouselResponse.from_rep(item)
                    content.append(element)

        if 'conversationId' in data:
            conversation_id = data['conversationId']
        else:
            conversation_id = None

        logging.warning("MODELS.MESSAGE default parameters set up for ".format(data['messageId']))

        return NotificationMessage(data['messageId'], conversation_id, data['channel'], data['userId'], data['timestamp'], content, metadata, data['project'])


class QuickReplyResponse:
    def __init__(self, button_text: str, button_id: str):
        self.type = "action"
        self.button_text = button_text
        self.button_id = button_id

    def to_repr(self) -> dict:
        return {
            'type': 'action',
            'buttonText': self.button_text,
            'buttonId': self.button_id
        }

    @staticmethod
    def from_rep(data: dict):

        button_text = None

        if "buttonText" in data:
            button_text = data['buttonText']

        button_id = None

        if "buttonId" in data:
            button_id = data['buttonId']

        return QuickReplyResponse(button_text, button_id)


class AttachmentResponse:
    def __init__(self, uri: str, alternative_text="", buttons=Optional[List[QuickReplyResponse]]):
        """
        Create an AttachmentResponse Object. An attachment response is a response containing only a media
        :param uri: uri of the media as a string
        :param alternative_text: the alternative text of the media. It is a string and it is displayed whenever the media cannot be loaded correctly. Usually, it is a description of the media
        :param buttons: a list of QuickReply responses (list of buttons to let the user perform quick actions). This field is optional
        """

        self.uri = uri
        self.alternative_text = alternative_text
        self.buttons = buttons

    def __repr__(self):
        return 'AttachmentResponse(uri {})'.format(self.uri)

    def to_repr(self) -> dict:

        buttons = []

        for button in self.buttons:
            if not isinstance(button, QuickReplyResponse):
                raise ValueError("the elements in the button list should be instances of QuickReplyResponse")
            else:
                buttons.append(button.to_repr())
        return {
            "type": "attachment",
            "uri": self.uri,
            "alternativeText": self.alternative_text,
            "buttons": buttons
        }

    @staticmethod
    def from_rep(data: dict):
        buttons = []
        for action in data['buttons']:
            a = QuickReplyResponse.from_rep(action)
            buttons.append(a)
        return AttachmentResponse(data['uri'], data['alternativeText'], buttons)


class CarouselResponse:
    def __init__(self, type, title, description, uri, buttons=Optional[List[QuickReplyResponse]]):
        self.type = type,
        self.title = title,
        self.description = description,
        self.uri = uri,
        self.buttons = buttons

        for action in buttons:
            if not isinstance(action, QuickReplyResponse):
                raise ValueError("buttons should be QuickReplyResponse objects")

    def to_repr(self) -> dict:
        buttons = []
        for action in self.buttons:
            buttons.append(QuickReplyResponse.to_repr(action))
        return{
            'type': self.type,
            'title': self.title,
            'description': self.description,
            'uri': self.uri,
            'buttons': buttons
        }

    @staticmethod
    def from_rep(data: dict):
        buttons = []
        for action in data['buttons']:
            a = QuickReplyResponse.from_rep(action)
            buttons.append(a)

        return CarouselResponse(data['type'], data['title'], data['description'], data['uri'], buttons)

