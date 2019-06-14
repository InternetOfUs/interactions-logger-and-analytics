import requests


class LoggingUtility:

    def __init__(self, service_host: str, service_port: int, project:str):
        self._access_point = service_host + ":" + str(service_port)
        self._project = project

    def add_request(self, message: str, message_type: str, message_id: str, user_id: str, conversation_id: str, structure_id: str, channel: str, timestamp: str, project: str, domain="", intent_name = "", intent_confidence = 0.0, entities=[], language="", metadata={}):
        """
        This method should be used to store a request message. A request message is a message from the user to the chatbot
        :param message: a string that indicates the text of the message
        :param message_type: should be "text" or "action" and indicates whether the user typed the message or clicked on a quick reply button. It is a string
        :param message_id: the id of the message. This field should be a string
        :param user_id: the id of the user. This filed should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param structure_id: the id of the structure (i.e., the id of the hotel involved in the conversation). This field is a string
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param timestamp: the timestamp of the message. This field should be a string
        :param project: the name of the project associated to the message. It is a string
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

        if conversation_id is None or conversation_id == "":
            raise ValueError("conversation_id cannot be empty")

        if structure_id is None or structure_id == "":
            raise ValueError("structure_id cannot be empty")

        if channel in None or channel == "":
            raise ValueError("channel cannot be empty")

        if project is None or project == "":
            raise ValueError("project cannot be empty")

        if message_type is None or message_type == "" or message_type.lower() != "text" or message_type.lower() != "action":
            raise ValueError("message type should be 'text' or 'action'")

        if timestamp is None or timestamp == "":
            raise ValueError("timestamp cannot be empty")

        if not isinstance(entities, list):
            raise ValueError("entities should be a list")

        if not isinstance(metadata, dict):
            raise ValueError("metadata should be a dictionary")

        if (intent_confidence != 0.0 or intent_confidence is None) and (intent_name == "" or intent_name is None):
            raise ValueError("intent confidence is defined but the intent name is empty")

        api_point = self._access_point + "/messages"

        intent_dict = {}
        if intent_name != "":
            intent_dict['name'] = intent_name
            intent_dict['confidence'] = intent_confidence

        temp_entities = []

        for entity in entities:
            if not isinstance(entity, Entity):
                raise ValueError("entities should be a list of Entity objects")
            else:
                temp_entities.append(Entity(entity).to_dict())

        message_generated = {
            "messageId": message_id,
            "channel": channel,
            "userId": user_id,
            "conversationId": conversation_id,
            "structureId": structure_id,
            "timestamp": timestamp,
            "content": {
                "type": message_type,
                "value": message
            },
            "domain": domain,
            "intent": intent_dict,
            "entities": temp_entities,
            "project": project.lower(),
            "language": language,
            "metadata": metadata,
            "type": "REQUEST"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=messages)

        return response.status_code, response.content

    def add_response(self, message_id: str, conversation_id: str, channel: str, user_id: str, structure_id: str, response_to: str, timestamp: str, project: str, content=[], metadata={}):
        """
        This method should be used to store a response message. A response message is a message from the bot to the user that represents an answer to a request message
        :param message_id: the id of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param user_id: the id of the user. This filed should be a string
        :param structure_id: the id of the structure (i.e., the id of the hotel involved in the conversation). This field is a string
        :param response_to: the id of the request that generated this message. It is a string
        :param timestamp: the timestamp of the message. This field should be a string
        :param project: the name of the project associated to the message. It is a string
        :param content: is an optional list describing the content of the message. The objects in the list can be of types TextResponse, QuickReplyResponse, AttachmentResponse, CarouselResponse
        :param metadata: an optional dictionary containing additional information.
        :return:
        """

        if message_id == "" or message_id is None:
            raise ValueError("message_id cannot be empty")

        if conversation_id == "" or conversation_id is None:
            raise ValueError("conversation_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if user_id == "" or user_id is None:
            raise ValueError("user_id cannot be empty")

        if structure_id == "" or structure_id is None:
            raise ValueError("structure_id cannot be empty")

        if response_to == "" or response_to is None:
            raise ValueError("response_to cannot be empty")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp cannot be empty")

        if project == "" or project is None:
            raise ValueError("project cannot be empty")

        if not isinstance(metadata, dict):
            raise ValueError("metadata must be a dictionary")

        for item in content:
            if not ( isinstance(item, TextResponse) or isinstance(item, AttachmentResponse) or isinstance(item, CarouselResponse) or isinstance(item, QuickReplyResponse)):
                raise ValueError("each item in content should have type TextResponse, AttachmentResponse, QuickReplyResponse or CarouselResponse")

        api_point = self._access_point + "/messages"

        message_generated = {
            "messageId": message_id,
            "conversationId": conversation_id,
            "structureId": structure_id,
            "channel": channel,
            "userId": user_id,
            "timestamp": timestamp,
            "content": content,
            "metadata": metadata,
            "project": project,
            "type": "RESPONSE"
        }

        messages = [message_generated]

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(api_point, headers=headers, json=messages)

        return response.status_code, response.content

    def add_notification(self, message_id: str, conversation_id:str, structure_id: str, channel: str, user_id: str, timestamp: str, project: str, content=[], metadata={}):
        """
        This method should be used to store a notification message. A notification message is a message from the bot to the user that represents an answer to a request message
        :param message_id: the id of the message. This field should be a string
        :param conversation_id: a string to identify the id of the conversation
        :param structure_id: the id of the structure (i.e., the id of the hotel involved in the conversation). This field is a string
        :param channel: it is a string that identifies the channel used by the user. Some example of channels are TELEGRAM, MESSENGER, and ALEXA
        :param user_id: the id of the user. This filed should be a string
        :param timestamp: the timestamp of the message. This field should be a string
        :param project: the name of the project associated to the message. It is a string
        :param content: is an optional list describing the content of the message. The objects in the list can be of types TextResponse, QuickReplyResponse, AttachmentResponse, CarouselResponse
        :param metadata: an optional dictionary containing additional information.
        :return:
        """

        if message_id == "" or message_id is None:
            raise ValueError("message_id cannot be empty")

        if conversation_id == "" or conversation_id is None:
            raise ValueError("conversation_id cannot be empty")

        if channel == "" or channel is None:
            raise ValueError("channel cannot be empty")

        if user_id == "" or user_id is None:
            raise ValueError("user_id cannot be empty")

        if structure_id == "" or structure_id is None:
            raise ValueError("structure_id cannot be empty")

        if timestamp == "" or timestamp is None:
            raise ValueError("timestamp cannot be empty")

        if project == "" or project is None:
            raise ValueError("project cannot be empty")

        if not isinstance(metadata, dict):
            raise ValueError("metadata must be a dictionary")

        for item in content:
            if not ( isinstance(item, TextResponse) or isinstance(item, AttachmentResponse) or isinstance(item, CarouselResponse) or isinstance(item, QuickReplyResponse)):
                raise ValueError("each item in content should have type TextResponse, AttachmentResponse, QuickReplyResponse or CarouselResponse")

        api_point = self._access_point + "/messages"

        message_generated = {
            "messageId": message_id,
            "conversationId": conversation_id,
            "structureId": structure_id,
            "channel": channel,
            "userId": user_id,
            "timestamp": timestamp,
            "content": content,
            "metadata": metadata,
            "project": project,
            "type": "NOTIFICATION"
        }

        messages = [message_generated]

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


class Entity:

    def __init__(self, type:str, value, confidence = 0.0):
        """
        Method to create an Entity
        :param type: the type of the entity (i.e., @city)
        :param value: the value of the entity (i.e., Boston)
        :param confidence: the confidence with which the entity has been detected. The default value for this parameter is 0.0
        """
        self.type = type
        self.value = value
        self.confidence = confidence

    def __repr__(self):
        return 'Entity(type {}, value {})'.format(self.type, self.value)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "value": self.value,
            "confidence": self.confidence
        }


class TextResponse:

    def __init__(self, value: str, buttons = []):
        """
        Create a TextResponse object. A text response can be seen as a standard message containing only text
        :param value: the text of the response
        :param buttons: an optional list of QuickReplyResponse objects
        """
        self.value = value
        self.buttons = buttons

    def __repr__(self):
        return 'TextResponse(value {})'.format(self.value)

    def to_dict(self) -> dict:
        buttons = []

        for button in self.buttons:
            if not isinstance(button, QuickReplyResponse):
                raise ValueError("the elements in the button list should be instances of QuickReplyResponse")
            else:
                buttons.append(button.to_dict())
        return {
            "type": "text",
            "value": self.value,
            "buttons": buttons
        }


class QuickReplyResponse:

    def __init__(self, button_text: str, button_id="", media_type="", media_uri=""):
        """
        Create a QuickReplyResponse object. A quick reply response is a button that suggest the user a quick action to perform.
        :param button_text: the text of the button
        :param button_id: optionally, the id of the button
        :param media_type: if there is an image associated to the button, declare it here. media_type should be "image" or "video". This field is optional
        :param media_uri: the URI of the media . This field is optional but must be declared if the media type is defined
        """

        if (not(media_type is None) or media_type != "") and (media_uri == "" or media_uri is None):
            raise ValueError("media_type is defined but media_uri is empty")

        self.button_text = button_text
        self.button_id = button_id
        self.media_type = media_type
        self.media_uri = media_uri

    def __repr__(self):
        return 'QuickReplyResponse(value {}, id {})'.format(self.button_text, self.button_id)

    def to_dict(self) -> dict:
        return {
            "type": "action",
            "buttonText": self.button_text,
            "buttonId": self.button_id,
            "mediaType": self.media_type,
            "mediaURI": self.media_uri
        }

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

    def to_dict(self) -> dict:

        buttons = []

        for button in self.buttons:
            if not isinstance(button, QuickReplyResponse):
                raise ValueError("the elements in the button list should be instances of QuickReplyResponse")
            else:
                buttons.append(button.to_dict())
        return {
            "type": "attachment",
            "uri": self.uri,
            "alternativeText": self.alternative_text,
            "buttons": buttons
        }

class CarouselResponse:

    def __init__(self, title:str, image_url:str, subtitle="", default_action={}, buttons = []):
        """
        Create a CarouselResponse Object. A carousel is a scrollable list of medias. As a best-practice, carousels should be used with no more that 6 elements and when the elements can be ranked.
        :param title: the title of the slide of the carousel
        :param image_url: the url of the media
        :param subtitle: eventually, a subtitle for the slide
        :param default_action: the default action the user may take. It is a dictionary and it is optional
        :param buttons: a list of QuickReply responses (list of buttons to let the user perform quick actions)
        """
        self.title = title
        self.image_url = image_url
        self.subtitle = subtitle
        self.default_action = default_action
        self.buttons = buttons

    def to_dict(self) -> dict:

        buttons = []

        for button in self.buttons:
            if not isinstance(button, QuickReplyResponse):
                raise ValueError("the elements in the button list should be instances of QuickReplyResponse")
            else:
                buttons.append(button.to_dict())

        return {
            "title": self.title,
            "imageUrl": self.image_url,
            "subtitle": self.subtitle,
            "defaultAction": self.default_action,
            "buttons": buttons
        }