import logging
import unittest

from awstin.awslambda import __name__ as EVENT_NAME
from awstin.awslambda import lambda_handler

EVENT = {
    "requestContext": {"requestId": "an ID"},
}
CONTEXT = {
    "memory_limit_in_mb": 333,
}


class TestEventDecorator(unittest.TestCase):
    def test_custom_event_handler_tuple(self):
        def custom_event_parser(event, context):
            request_id = event["requestContext"]["requestId"]
            memory_limit = context["memory_limit_in_mb"]
            return request_id, memory_limit

        @lambda_handler(custom_event_parser)
        def handle_custom_event(request_id, memory_limit):
            self.assertEqual(request_id, "an ID")
            self.assertEqual(memory_limit, 333)

            return "Result!"

        # Externally the handler takes the normal signature
        with self.assertLogs(EVENT_NAME, logging.INFO) as logs:
            result = handle_custom_event(EVENT, CONTEXT)

        self.assertEqual(result, "Result!")

        self.assertEqual(len(logs.records), 2)
        record1, record2 = logs.records

        self.assertEqual(f"Event: {EVENT!r}", record1.getMessage())
        self.assertEqual("Result: 'Result!'", record2.getMessage())

    def test_custom_event_handler_dict(self):
        def custom_event_parser(event, context):
            request_id = event["requestContext"]["requestId"]
            memory_limit = context["memory_limit_in_mb"]
            return dict(rid=request_id, mem=memory_limit)

        @lambda_handler(custom_event_parser)
        def handle_custom_event(mem, rid):
            self.assertEqual(rid, "an ID")
            self.assertEqual(mem, 333)

            return "Result!"

        result = handle_custom_event(EVENT, CONTEXT)
        self.assertEqual(result, "Result!")

    def test_custom_event_handler_value(self):
        def custom_event_parser(event, context):
            memory_limit = context["memory_limit_in_mb"]
            return memory_limit

        @lambda_handler(custom_event_parser)
        def handle_custom_event(limit):
            self.assertEqual(limit, 333)

            return "Result!"

        result = handle_custom_event(EVENT, CONTEXT)
        self.assertEqual(result, "Result!")
