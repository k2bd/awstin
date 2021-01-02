===================================================
Integration Testing :mod:`awstin.dynamodb` Projects
===================================================

Note that when integration testing DynamoDB code in :mod:`awstin`, the
``TEST_DYNAMODB_ENDPOINT`` environment variable should be set to the endpoint
of a dockerized DynamoDB instance.

The central tool for building integration tests for projects using DynamoDB in
:mod:`awstin` is :func:`awstin.dynamodb.testing.temporary_dynamodb_table`.
This context manager creates a DynamoDB table with the provided information
on entry, and destroys it on exit. It ensures that these operations are
completed before entry and exit to prevent any race conditions.

It's easy to build fixtures or mixins on top of this context manager to produce
tables in whatever state you'd like to emulate production scenarios for
testing.
