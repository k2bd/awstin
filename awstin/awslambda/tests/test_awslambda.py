import logging
import unittest

from awstin.awslambda.api import LambdaEvent
from awstin.awslambda.api import __name__ as EVENT_NAME
from awstin.awslambda.api import lambda_handler


class CustomEvent(LambdaEvent):
    def __init__(self, request_id, memory_limit):
        self.request_id = request_id
        self.memory_limit = memory_limit

    @classmethod
    def _from_event(cls, event, context):
        request_id = event["requestContext"]["requestId"]
        memory_limit = context["memory_limit_in_mb"]
        return cls(request_id, memory_limit)


class TestLambdaEvent(unittest.TestCase):
    def test_custom_event_from_event(self):
        event = {
            "requestContext": {"requestId": "id1"},
        }
        context = {
            "memory_limit_in_mb": 9001,
        }
        custom_event = CustomEvent.from_event(event, context)

        self.assertEqual(custom_event.request_id, "id1")
        self.assertEqual(custom_event.memory_limit, 9001)

        with self.assertLogs(EVENT_NAME, logging.WARNING) as logs:
            self.assertEqual(custom_event.raw_event, event)

        self.assertEqual(len(logs.records), 1)
        record, = logs.records
        self.assertIn(
            "Using the raw event is discouraged!",
            record.getMessage()
        )

        with self.assertLogs(EVENT_NAME, logging.WARNING) as logs:
            self.assertEqual(custom_event.raw_context, context)

        self.assertEqual(len(logs.records), 1)
        record, = logs.records
        self.assertIn(
            "Using the raw context is discouraged!",
            record.getMessage()
        )


class TestEventDecorator(unittest.TestCase):
    def test_custom_event_handler(self):
        event = {
            "requestContext": {"requestId": "an ID"},
        }
        context = {
            "memory_limit_in_mb": 333,
        }

        has_been_called = False

        @lambda_handler(CustomEvent)
        def handle_custom_event(custom_event):
            self.assertIsInstance(custom_event, CustomEvent)

            self.assertEqual(custom_event.request_id, "an ID")
            self.assertEqual(custom_event.memory_limit, 333)

            nonlocal has_been_called
            has_been_called = True

            return "Result!"

        # Externally the handler takes the normal signature
        with self.assertLogs(EVENT_NAME, logging.INFO) as logs:
            result = handle_custom_event(event, context)

        self.assertTrue(has_been_called)

        self.assertEqual(result, "Result!")

        self.assertEqual(len(logs.records), 2)
        record1, record2 = logs.records

        self.assertEqual(f"Event: {event!r}", record1.getMessage())
        self.assertEqual("Result: 'Result!'", record2.getMessage())
