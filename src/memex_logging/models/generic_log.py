class GeneralLog:

    def __init__(self, source: str, dtime: str, trace_id: str, message: str, metadata: dict, project: str):
        self._source = source
        self._dtime = dtime
        self._trace_id = trace_id
        self._message = message
        self._metadata = metadata
        self._project = project

        if not isinstance(metadata, dict):
            raise ValueError("metadata should be a dictionary")

    @staticmethod
    def from_rep(data: dict):
        return GeneralLog(data['source'], data['dtime'], data['traceId'], data['message'], data['metadata'], data['project'])

    def to_repr(self) -> dict:
        return {
            'source': self._source,
            'datetime': self._dtime,
            'traceId': self._trace_id,
            'message': self._message,
            'metadata': self._metadata,
            'project': self._project
        }
