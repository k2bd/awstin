==============================================
Getting started with DynamoDB in :mod:`awstin`
==============================================

This tutorial follows the "Getting Started" guide for DynamoDB and Python in
the :mod:`boto3` docs. That reference can be found here_.

Equivalent tutorials are presented for each section, except for the first and
last sections on creating and deleting tables. These functions of :mod:`boto3`
are out-of-scope for :mod:`awstin`, leaving infrastructure management to
dedicated IaC frameworks or to :mod:`boto3`.

In each case, the structure of the examples are kept as comparable as possible
to the examples in the :mod:`boto3` docs.

Note that when using DynamoDB in :mod:`awstin`, either the ``AWS_REGION``
(in production) or the ``TEST_DYNAMODB_ENDPOINT`` (in integration tests)
environment variable should be set. This will be used to connect either to the
real AWS DynamoDB service or to a testing instance of DynamoDB.

.. toctree::
   :numbered:
   :maxdepth: 2

   define_models/index
   load_sample_data/index
   crud_operations/index
   query_and_scan/index


.. _here: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.html
