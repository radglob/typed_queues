import collections


class DoesNotMatchSchemaError(Exception):
    message = 'Given message doesn\'t match the schema of the queue.'


def identity(x):
    return x


class TypedQueue(collections.deque):

    def __init__(self, in_schema, out_schema=None, convert=None):
        self._in_schema = in_schema
        self._out_schema = out_schema or in_schema
        self._convert = convert or identity

    def send_message(self, message):
        if self._in_schema.validates(message):
            self.append(message)
        else:
            raise DoesNotMatchSchemaError()

    def receive_message(self):
        message = self.pop()
        return self._convert(message)
