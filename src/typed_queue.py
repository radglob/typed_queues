import collections


def identity(x):
    return x


class MessageSchema(object):

    def __init__(self, schema):
        self._schema = schema

    def validates(self, message):
        # First, check that keys match.
        assert self._schema.keys() == message.keys()
        # Check values types.
        for k, v in message.items():
            assert type(message[k]) == self._schema[k]
        return True


class TypedQueue(collections.deque):

    def __init__(self, in_schema, convert=None):
        self._in_schema = in_schema
        self._convert = convert or identity

    def send_message(self, message):
        if self._in_schema.validates(message):
            self.append(message)
            return True

    def receive_message(self):
        message = self.pop()
        return self._convert(message)
