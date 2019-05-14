from __future__ import absolute_import


class Intent:
    def __init__(self, name: str, confidence: float):
        self._name = name
        self._confidence = confidence

    def to_repr(self) -> dict:
        return {
            'name': self._name,
            'confidence': self._confidence
        }
    # TODO update python + import annotaion
    @staticmethod
    def from_rep(data: dict) -> Intent:
        return Intent(data['name'], data['confidence'])


class RequestMessage:

    def __init__(self, trace_id: str, sentence: str, domain: str, intent: Intent, entities: dict, project: str,
                 language: str) -> None:
        self._trace_id = trace_id
        self._sentence = sentence
        self._domain = domain
        self._intent = intent
        self._entities = entities
        self._project = project
        self._language = language

        if not isinstance(intent, Intent):
            raise ValueError('intent type should be Intent')

        if not isinstance(entities, dict):
            raise ValueError('entities type should be dictionary')

    def to_repr(self) -> dict:
        return {
            'traceId': self._trace_id,
            'sentence': self._sentence,
            'domain': self._domain,
            'intent': self._intent.to_repr(),
            'entities': self._entities,
            'project': self._project,
            'language': self._project
        }

    @staticmethod
    def from_rep(data: dict):
        intent = Intent.from_rep(data['intent'])
        return RequestMessage(data['traceId'], data['sentence'], data['domain'], intent, data['entities'],
                              data['project'], data['language'])


# TODO check models (2)
class ResponseResource:
    def __init__(self, resource_id: str, type: str, metadata: dict):
        self._resourceId = resource_id
        self._type = type
        self._metadata = metadata

        if not isinstance(metadata, dict):
            raise ValueError('metadata type should be dictionary')

    def to_repr(self) -> dict:
        return{
            'resourceId': self._resourceId,
            'type': self._type,
            'metadata': self._metadata
        }

    @staticmethod
    def from_rep(data: dict):
        return ResponseResource(data['resourceId'], data['type'], data['metadata'])


class ResponseMessage:
    def __init__(self, trace_id: str, sentence: str, resources: list):
        self._trace_id = trace_id
        self._sentence = sentence
        self._resources = resources

        for resource in resources:
            if not isinstance(resource, ResponseResource):
                raise ValueError('resource type should be ResponseResource')

    def to_repr(self) -> dict:
        return {
            'traceId': self._trace_id,
            'sentence': self._sentence,
            'resources': self._resources
        }

    @staticmethod
    def from_rep(data: dict):
        return ResponseMessage(data['traceId'], data['sentence'], data['resources'])


class NotificationMessage:
    def __init__(self, trace_id: str, sentence: str, metadata: dict):
        self._trace_id = trace_id,
        self._sentence = sentence,
        self._metadata = metadata

        if not isinstance(metadata, dict):
            raise ValueError('metadata type should be dictionary')

    def to_repr(self) -> dict:
        return {
            'traceId': self._trace_id,
            'sentence': self._sentence,
            'metadata': self._metadata
        }

    @staticmethod
    def from_rep(data: dict):
        return NotificationMessage(data['traceId'], data['sentence'], data['metadata'])

