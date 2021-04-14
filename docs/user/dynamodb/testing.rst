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

For projects with IaC, integration testing can introduce multiple sources of
truth for the infrastructure that can get out-of-sync. To help combat this,
:mod:`awstin` provides :func:`awstin.dynamodb.testing.create_serverless_tables`.
This will create tables based on the given `serverless.yml` file. Note that this
functionality is still basic, and can't intepret variables in these resources yet.

It's easy to build fixtures or mixins on top of these context managers to
produce tables in whatever state you'd like to emulate production scenarios for
testing.
