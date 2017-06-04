import datetime
import pytest

from src.typed_queue import (
    MessageSchema,
    TypedQueue
)


def test_typed_queue_send_message_with_valid_message():
    schema = MessageSchema({'date': datetime.date,
                            'count': int})
    queue = TypedQueue(schema)
    success = queue.send_message({'date': datetime.date.today(),
                                  'count': 12})
    assert success is True


def test_typed_queue_send_message_with_invalid_message():
    schema = MessageSchema({'date': datetime.date,
                            'count': int})
    queue = TypedQueue(schema)
    with pytest.raises(AssertionError):
        queue.send_message({'count': 12,
                            'color': 'fred'})


def test_typed_queue_receive_message_identity():
    schema = MessageSchema({'date': datetime.date,
                            'count': int})
    queue = TypedQueue(schema)
    in_message = {'date': datetime.date.today(),
                  'count': 12}
    queue.send_message(in_message)
    out_message = queue.receive_message()
    assert in_message == out_message


def test_typed_queue_receive_message_converted():
    in_schema = MessageSchema({'date': datetime.date,
                               'count': int})

    def convert(message):
        out_message = {}
        out_message['old_date'] = message['date']
        out_message['new_date'] = (
            message['date'] + datetime.timedelta(days=message['count']))
        return out_message

    queue = TypedQueue(in_schema, convert=convert)
    queue.send_message({'date': datetime.date.today(), 'count': 12})
    out_message = queue.receive_message()
    expected = {'old_date': datetime.date.today(),
                'new_date': (datetime.date.today() +
                             datetime.timedelta(days=12))}
    assert out_message == expected
