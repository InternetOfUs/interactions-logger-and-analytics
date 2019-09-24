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

        return Intent(name, confidence)


class UserInfoRequest:
    def __init__(self, type: str, value: str) -> None:
        self.type = type
        self.value = value

    def to_repr(self) -> dict:
        return{
            'type': self.type,
            'value': self.value
        }

    @staticmethod
    def from_rep(data: dict) -> UserInfoRequest:
        if 'type' not in data:
            raise ValueError("A UserInfoRequest must contain a type. 'type' is missing")
        if 'value' not in data:
            raise ValueError("A UserInfoRequest must contain a value. 'value' is missing")
        return UserInfoRequest(data['type'], data['value'])


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


class ActionResponse:
    def __init__(self, button_text: str, button_id: str):
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

        if "buttonText" in data:
            button_text = data['buttonText']
        else:
            raise ValueError("An action must have the Text of the button")

        button_id = None

        if "buttonId" in data:
            button_id = data['buttonId']

        return ActionResponse(button_text, button_id)


class MultiActionResponse:
    def __init__(self, buttons=Optional[List[ActionResponse]]):
        self.buttons = buttons

    def to_repr(self) -> dict:
        buttons = []

        for button in self.buttons:
            if not isinstance(button, ActionResponse):
                raise ValueError("the elements in the button list should be instances of QuickReplyResponse")
            else:
                buttons.append(button.to_repr())

        return {
            'type': 'multiaction',
            'buttons': buttons
        }

    @staticmethod
    def from_rep(data: dict):
        buttons = []
        if 'buttons' in data:
            for action in data['buttons']:
                a = ActionResponse.from_rep(action)
                buttons.append(a)
        return MultiActionResponse(buttons)


class TextualRequest:
    def __init__(self, value: str):
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


class TextualResponse:
    def __init__(self, value: str, buttons=Optional[List[ActionResponse]]) -> None:
        self.value = value
        self.buttons = buttons

    def to_repr(self) -> dict:
        buttons = []

        for button in self.buttons:
            if not isinstance(button, ActionResponse):
                raise ValueError("the elements in the button list should be instances of QuickReplyResponse")
            else:
                buttons.append(button.to_repr())

        return {
            'type': 'text',
            'value': self.value,
            'buttons': buttons
        }

    @staticmethod
    def from_rep(data: dict) -> TextualResponse:
        buttons = []
        if 'buttons' in data:
            for action in data['buttons']:
                a = ActionResponse.from_rep(action)
                buttons.append(a)
        return TextualResponse(data['value'], buttons)


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


class AttachmentResponse:
    def __init__(self, uri: str, alternative_text="", buttons=Optional[List[ActionResponse]]):
        """
        Create an AttachmentResponse Object. An attachment response is a response containing only a media
        :param uri: uri of the media as a string
        :param alternative_text: the alternative text of the media. It is a string and it is displayed whenever the media cannot be loaded correctly. Usually, it is a description of the media
        :param buttons: a list of QuickReply responses (list of buttons to let the user perform quick actions). This field is optional
        """
        self.uri = uri
        self.alternative_text = alternative_text
        self.buttons = buttons

    def to_repr(self) -> dict:

        buttons = []

        for button in self.buttons:
            if not isinstance(button, ActionResponse):
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
        if 'buttons' in data:
            for action in data['buttons']:
                a = ActionResponse.from_rep(action)
                buttons.append(a)
        alt = None
        if 'alternativeText' in data:
            alt = data['alternativeText']
        if 'uri' not in data:
            raise ValueError("An AttachmentRequest object must contain an uri")
        return AttachmentResponse(data['uri'], alt, buttons)


class CarouselCardResponse:
    def __init__(self, title: str, imageUrl: str, subtitle: str, defaultAction: dict, buttons=Optional[List[ActionResponse]]):
        self.title = title
        self.imageUrl = imageUrl
        self.subtitle = subtitle
        self.default_action = defaultAction
        self.buttons = buttons

    def to_repr(self) -> dict:
        buttons = []
        for action in self.buttons:
            buttons.append(ActionResponse.to_repr(action))
        return{
            'title': self.title,
            'imageUrl': self.imageUrl,
            'subtitle': self.subtitle,
            'defaultAction': self.default_action,
            'buttons': buttons
        }

    @staticmethod
    def from_rep(data: dict):
        buttons = []
        if 'buttons' in data:
            for action in data['buttons']:
                a = ActionResponse.from_rep(action)
                buttons.append(a)

        if 'title' not in data:
            raise ValueError("each card should have a title")

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


class CarouselResponse:
    def __init__(self, cards=List[CarouselCardResponse]):
        self.cards = cards

    def to_repr(self) -> dict:
        cards = []

        for card in self.cards:
            if not isinstance(card, CarouselCardResponse):
                raise ValueError("each card should be instances of CarouselCardResponse")
            else:
                cards.append(card.to_repr())

        return {
            'type': 'carousel',
            'cards': cards
        }

    @staticmethod
    def from_rep(data: dict) -> CarouselResponse:
        cards = []
        for card in data['cards']:
            c = CarouselCardResponse.from_rep(card)
            cards.append(c)

        return CarouselResponse(cards)


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
            if not (isinstance(content, TextualRequest) or isinstance(content, ActionRequest) or isinstance(content, AttachmentRequest) or isinstance(content, LocationRequest) or isinstance(content, UserInfoRequest)):
                raise ValueError("content should be TextRequest, ActionRequest, AttachmentRequest, UserInfoRequest or LocationRequest")

        logging.info("MODELS.MESSAGE  content check passed for ".format(message_id))

        for entity in entities:
            if not isinstance(entity, Entity):
                raise ValueError('entities should contains only object with type Entity')

        logging.info("MODELS.MESSAGE  entity check passed for ".format(message_id))

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

        logging.info("MODELS.MESSAGE starting logging a REQUEST message {}".format(data['messageId']))

        if 'intent' in data:
            intent = Intent.from_rep(data['intent'])
        else:
            intent_value = {
                'name': None,
                'confidence': None
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
        user_info_fields = ['firstName', 'lastName', 'profilePic', 'locale', 'timezone', 'gender', 'isPaymentEnable', 'userId']
        if "content" in data:
            if data['content']['type'] == "text":
                content = TextualRequest.from_rep(data['content'])
            elif data['content']['type'] == "action":
                content = ActionRequest.from_rep(data['content'])
            elif data['content']['type'] == "attachment":
                content = AttachmentRequest.from_rep(data['content'])
            elif data['content']['type'] == "location":
                content = LocationRequest.from_rep(data['content'])
            elif data['content']['type'] in user_info_fields:
                content = UserInfoRequest.from_rep(data['content'])
            else:
                raise ValueError("An unknown content type is in the message body")

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

        logging.warning("MODELS.MESSAGE default parameters set up for {}".format(data['messageId']))

        return RequestMessage(data['messageId'], data['channel'], data['userId'], conversation_id, data['timestamp'], content, domain, intent, entities,
                              data['project'], language, metadata)


class ResponseMessage:
    def __init__(self, message_id: str, conversation_id: str, channel: str, user_id: str, response_to: str, timestamp: str, content, metadata: dict, project: str):

        logging.info("MODELS.MESSAGE creating a ResponseMessage object for {}".format(message_id))

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

        if not (isinstance(content, MultiActionResponse) or isinstance(content, CarouselResponse) or isinstance(content, AttachmentResponse) or isinstance(content, TextualResponse) or isinstance(content, LocationRequest)):
            raise ValueError("response should contains only elements from QuickReplyResponse, CarouselResponse, AttachmentResponse, TextualResponse")

        logging.info("MODELS.MESSAGE  content check passed for ".format(message_id))

        if metadata is not None:
            if not isinstance(metadata, dict):
                raise ValueError('metadata type should be dictionary')

        logging.info("MODELS.MESSAGE  metadata check passed for ".format(message_id))

    def to_repr(self) -> dict:
        return {
            'messageId': self.message_id,
            'conversationId': self.conversation_id,
            'channel': self.channel,
            'userId': self.user_id,
            'responseTo': self.response_to,
            'timestamp': self.timestamp,
            'content': self.content.to_repr(),
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

        content = None
        if 'content' in data:
            if data['content']['type'] == 'text':
                content = TextualResponse.from_rep(data['content'])
            elif data['content']['type'] == 'location':
                content = LocationRequest.from_rep(data['content'])
            elif data['content']['type'] == 'multiaction':
                content = MultiActionResponse.from_rep(data['content'])
            elif data['content']['type'] == 'attachment':
                content = AttachmentResponse.from_rep(data['content'])
            elif data['content']['type'] == 'carousel':
                content = CarouselResponse.from_rep(data['content'])
            else:
                raise ValueError("an unknown type of content is in the body of the message")

        if 'conversationId' in data:
            conversation_id = data['conversationId']
        else:
            conversation_id = None

        if 'project' in data:
            project = data['project']
        else:
            project = "memex"

        logging.warning("MODELS.MESSAGE default parameters set up for ".format(data['messageId']))

        return ResponseMessage(data['messageId'], conversation_id, data['channel'], data['userId'], data['responseTo'], data['timestamp'], content, metadata, project)


class NotificationMessage:
    def __init__(self, message_id: str, conversation_id: str, channel: str, user_id: str, timestamp: str, content, metadata: dict, project: str):

        logging.info("MODELS.MESSAGE creating a NotificationMessage object for ".format(message_id))

        self.message_id = message_id
        self.conversation_id = conversation_id
        self.channel = channel
        self.user_id = user_id
        self.timestamp = timestamp
        self.content = content
        self.metadata = metadata
        self.project = project
        self.type = "NOTIFICATION"

        if not (isinstance(content, MultiActionResponse) or isinstance(content, CarouselResponse) or isinstance(content, AttachmentResponse) or isinstance(content, TextualResponse) or isinstance(content, LocationRequest)):
            raise ValueError("response should contains only elements from QuickReplyResponse, CarouselResponse, AttachmentResponse, TextualResponse")

        logging.info("MODELS.MESSAGE  content check passed for ".format(message_id))

        if metadata is not None:
            if not isinstance(metadata, dict):
                raise ValueError('metadata type should be dictionary')

        logging.info("MODELS.MESSAGE  metadata check passed for ".format(message_id))

    def to_repr(self) -> dict:

        return {
            'messageId': self.message_id,
            'conversationId': self.conversation_id,
            'channel': self.channel,
            'userId': self.user_id,
            'timestamp': self.timestamp,
            'content': self.content.to_repr(),
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

        content = None
        if 'content' in data:
            if data['content']['type'] == 'text':
                content = TextualResponse.from_rep(data['content'])
            elif data['content']['type'] == 'location':
                content = LocationRequest.from_rep(data['content'])
            elif data['content']['type'] == 'multiaction':
                content = MultiActionResponse.from_rep(data['content'])
            elif data['content']['type'] == 'attachment':
                content = AttachmentResponse.from_rep(data['content'])
            elif data['content']['type'] == 'carousel':
                content = CarouselResponse.from_rep(data['content'])
            else:
                raise ValueError("an unknown type of content is in the body of the message")

        if 'conversationId' in data:
            conversation_id = data['conversationId']
        else:
            conversation_id = None

        logging.warning("MODELS.MESSAGE default parameters set up for ".format(data['messageId']))

        return NotificationMessage(data['messageId'], conversation_id, data['channel'], data['userId'], data['timestamp'], content, metadata, data['project'])
