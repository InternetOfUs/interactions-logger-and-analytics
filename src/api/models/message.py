class ErrorMessage:

    def __init__(self,trace_id: str, time: str, module_name: str, message: str):
        self.trace_id = trace_id
        self.time = time
        self.module_name = module_name
        self.message = message

    def from_rep(self, data:dict) -> ErrorMessage:
        return ErrorMessage(data['trace_id'], data['timestamp'], data['module_name'], data['message'])

    def to_repr(self) -> dict:
        object = {
            'traceId' : self.trace_id,
            'timestamp' : self.time,
            'moduleName' : self.module_name,
            'message' : self.message
        }
        return object


class PerformanceMessage:

    def __init__(self,trace_id: str, time: str, module_name: str, performance_metric: str, performance_value: float):
        self.trace_id = trace_id
        self.time = time
        self.module_name = module_name
        self.performance_metric = performance_metric
        self.performance_value = performance_value
