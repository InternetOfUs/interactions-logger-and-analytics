from __future__ import absolute_import, annotations


class Intent:
    def __init__(self, name: str, confidence: float) -> None:
        self._name = name
        self._confidence = confidence

    def to_repr(self) -> dict:
        return {
            'name': self._name,
            'confidence': self._confidence
        }

    @staticmethod
    def from_rep(data: dict) -> Intent:
        return Intent(data['name'], data['confidence'])

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
        return TextualRequest(data['value'])


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
        return ActionRequest(data['value'])


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
        return AttachmentRequest(data['uri'],data['alternativeText'])


class Entity:
    def __init__(self, type: str, value: str, confidence: float):
        self._type = type
        self._value = value
        self._confidence = confidence

    def to_repr(self) -> dict:
        return {
            'type': self._type,
            'value': self._value,
            'confidence': self._confidence
        }

    @staticmethod
    def from_rep(data: dict) -> Entity:
        return Entity(data['type'], data['value'], data['confidence'])


class RequestMessage:

    def __init__(self, message_id: str, channel: str, user_id: str, conversation_id:str, timestamp:str, content, domain:str,  intent: Intent, entities: list, project: str,
                 language: str, metadata:dict) -> None:
        self._message_id = message_id
        self._channel = channel
        self._user_id = user_id
        self._conversation_id = conversation_id
        self._timestamp = timestamp
        self._content = content
        self._domain = domain
        self._intent = intent
        self._entities = entities
        self._project = project
        self._language = language
        self._metadata = metadata

        if not isinstance(intent, Intent):
            raise ValueError('intent type should be Intent')

        if not isinstance(metadata, dict):
            raise ValueError('metadata type should be dictionary')

        if not (isinstance(content, TextualRequest) or isinstance(content, ActionRequest) or isinstance(content, AttachmentRequest)):
            raise ValueError("content should be TextRequest, ActionRequest or AttachmentRequest")

        for entity in entities:
            print(type(entity))
            if not isinstance(entity, Entity):
                raise ValueError('entities should contains only object with type Entity')

    def to_repr(self) -> dict:
        entities = []
        for entity in self._entities:
            entities.append(entity.to_repr())
        return {
            'messageId': self._message_id,
            'channel': self._channel,
            'userId': self._user_id,
            'conversationId': self._conversation_id,
            'timestamp': self._timestamp,
            'content': self._content.to_repr(),
            'domain': self._domain,
            'intent': self._intent.to_repr(),
            'entities': entities,
            'project': self._project,
            'language': self._language,
            'metadata': self._metadata,
            'type': 'REQUEST'
        }

    @staticmethod
    def from_rep(data: dict):
        if 'intent' in data:
            intent = Intent.from_rep(data['intent'])
        else:
            intent_value = {
                'name': 'nan',
                'confidence': 0
            }
            intent = Intent.from_rep(intent_value)

        if 'domain' in data:
            domain = data['domain']
        else:
            domain = "undefined"

        if 'conversationId' in data:
            conversation_id = data['conversationId']
        else:
            conversation_id = "undefined"

        if data['content']['type'] == "text":
            content = TextualRequest.from_rep(data['content'])
        elif data['content']['type'] == "action":
            content = ActionRequest.from_rep(data['content'])
        elif data['content']['type'] == "attachment":
            content = AttachmentRequest.from_rep(data['content'])


        entities = []
        for entity in data['entities']:
            e = Entity.from_rep(entity)
            entities.append(e)

        return RequestMessage(data['messageId'], data['channel'], data['userId'],conversation_id,data['timestamp'],content,domain, intent, entities,
                              data['project'], data['language'], data['metadata'])


class ResponseMessage:
    def __init__(self, message_id: str, conversation_id: str, channel: str, user_id:str, response_to:str, timestamp:str, content:list, metadata:dict, project:str):

        self._message_id = message_id
        self._conversation_id = conversation_id
        self._channel = channel
        self._user_id = user_id
        self._response_to = response_to
        self._timestamp = timestamp
        self._content = content
        self._metadata = metadata
        self._project = project
        self._type = "RESPONSE"

        for cont in content:
            if not (isinstance(cont, QuickReplyResponse) or isinstance(cont,CarouselResponse) or isinstance(cont, AttachmentResponse) or isinstance(cont,TextResponse)):
                raise ValueError("response should contains only elements from QuickReplyResponse, CarouselResponse, AttachmentResponse, TextResponse")

        if not isinstance(metadata, dict):
            raise ValueError("metadata should be a dictionary")

    def to_repr(self) -> dict:
        content_list = []
        for c in self._content:
            content_list.append(c.to_repr())

        return {
            'messageId': self._message_id,
            'conversationId': self._conversation_id,
            'channel': self._channel,
            'userId': self._user_id,
            'responseTo': self._response_to,
            'timestamp': self._timestamp,
            'content': content_list,
            'metadata': self._metadata,
            'project': self._project,
            'type': self._type
        }

    @staticmethod
    def from_rep(data: dict):
        content = []
        for item in data['content']:
            if item['type'] == 'text':
                element = TextResponse.from_rep(item)
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

        return ResponseMessage(data['messageId'], data['conversationId'], data['channel'], data['userId'], data['responseTo'], data['timestamp'], content, data['metadata'], data['project'])


class NotificationMessage:
    def __init__(self, message_id: str, conversation_id: str, channel: str, user_id:str, timestamp:str, content:list, metadata:dict, project:str):

        self._message_id = message_id
        self._conversation_id = conversation_id
        self._channel = channel
        self._user_id = user_id
        self._timestamp = timestamp
        self._content = content
        self._metadata = metadata
        self._project = project
        self._type = type

        for cont in content:
            if not (isinstance(cont, QuickReplyResponse) or isinstance(cont,CarouselResponse) or isinstance(cont, AttachmentResponse) or isinstance(cont,TextResponse)):
                raise ValueError("response should contains only elements from QuickReplyResponse, CarouselResponse, AttachmentResponse, TextResponse")

        if not isinstance(metadata, dict):
            raise ValueError("metadata should be a dictionary")

    def to_repr(self) -> dict:
        content_list = []
        for c in self._content:
            content_list.append(c.to_repr())

        return {
            'messageId': self._message_id,
            'conversationId': self._conversation_id,
            'channel': self._channel,
            'userId': self._user_id,
            'timestamp': self._timestamp,
            'content': content_list,
            'metadata': self._metadata,
            'project': self._project,
            'type': self._type
        }

    @staticmethod
    def from_rep(data: dict):
        content = []
        for item in data['content']:
            if item['type'] == 'text':
                element = TextResponse.from_rep(item)
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

        return NotificationMessage(data['messageId'], data['conversationId'], data['channel'], data['userId'], data['timestamp'], content, data['metadata'], data['project'])

class AttachmentResponse:
    def __init__(self, uri: str, alternative_text: str, buttons=[]):
        """
        Create an AttachmentResponse Object. An attachment response is a response containing only a media
        :param uri: uri of the media as a string
        :param alternative_text: the alternative text of the media. It is a string and it is displayed whenever the media cannot be loaded correctly. Usually, it is a description of the media
        :param buttons: a list of QuickReply responses (list of buttons to let the user perform quick actions)
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
        actions = []
        for action in data['action']:
            a = QuickReplyResponse.from_rep(action)
            actions.append(a)
        return AttachmentResponse(data['uri'],data['alternativeText'],actions)


class CarouselResponse:
    def __init__(self, type, title, description, uri, actions:list):
        self._type = type,
        self._title = title,
        self._description = description,
        self._uri = uri,
        self._actions = actions

        for action in actions:
            if not isinstance(action, QuickReplyResponse):
                raise ValueError("actions should contains only object with type ActionResponse")

    def to_repr(self) -> dict:
        actions = []
        for action in self._actions:
            actions.append(QuickReplyResponse.to_repr(action))
        return{
            'type': self._type,
            'title': self._title,
            'description': self._description,
            'uri': self._uri,
            'actions': actions
        }

    @staticmethod
    def from_rep(data: dict):
        actions = []
        for action in data['action']:
            a = QuickReplyResponse.from_rep(action)
            actions.append(a)

        return CarouselResponse(data['type'], data['title'], data['description'], data['uri'], actions)


class TextResponse:
    def __init__(self, type, value) -> None:
        self._type = type
        self._value = value

    def to_repr(self) -> dict:
        return {
            'type': self._type,
            'value': self._value
        }

    @staticmethod
    def from_rep(data: dict):
        return TextResponse(data['type'], data['value'])


class QuickReplyResponse:
    def __init__(self, type:str, button_text: str, button_id: str, media_type: str, media_uri: str):
        self._type = type
        self._button_text = button_text
        self._button_id = button_id
        self._media_type = media_type
        self._media_uri = media_uri

    def to_repr(self) -> dict:
        return {
            'type': self._type,
            'buttonText': self._button_text,
            'buttonId': self._button_id,
            'mediaType': self._media_type,
            'mediaURI': self._media_uri
        }

    @staticmethod
    def from_rep(data: dict):
        return QuickReplyResponse(data['type'], data['buttonText'], data['buttonId'], data['mediaType'], data['mediaURI'])


