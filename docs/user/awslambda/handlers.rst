=======================
Writing Lambda Handlers
=======================

Lambda handlers can be wrapped with the :func:`awstin.awslambda.lambda_handler`
decorator factory, which accepts a parser function as an argument. The parser
should accept an AWS event and context, and should return inputs to the wrapped
function as a tuple (to be passed in as args) or dict (to be passed in as
kwargs).

.. code-block:: python

    from awstin.awslambda import lambda_handler

    def event_parser(event, context):
        request_id = event["requestContext"]["requestId"]
        memory_limit = context["memory_limit_in_mb"]
        return request_id, memory_limit


    @lambda_handler(event_parser)
    def handle_custom_event(request_id, memory_limit):
        print(request_id)
        print(memory_limit)

In this way, the event parsing and business logic of Lambda handlers are kept
separate.
