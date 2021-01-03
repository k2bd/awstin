=========================
Accessing DynamoDB Tables
=========================

Once data models are defined, they can be used to interact with DynamoDB
tables. This is via the :class:`awstin.dynamodb.DynamoDB` class, which connects
to DynamoDB either via the ``AWS_REGION`` (in production) or the
``TEST_DYNAMODB_ENDPOINT`` (in integration tests) environment variable. Tables
are accessed from the :class:`awstin.dynamodb.DynamoDB` instance via indexing
by :class:`awstin.dynamodb.DynamoModel` subclasses.

.. code-block:: python

    from awstin.dynamodb import DynamoDB


    dynamodb = DynamoDB()
    table = dynamodb[Movie]
