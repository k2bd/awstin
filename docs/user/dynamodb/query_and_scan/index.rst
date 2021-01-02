.. _query-and-scan:

==============================
Querying and Scanning the Data
==============================

A query/condition syntax is provided by :mod:`awstin`. This is similar to the
syntax provided by :mod:`sqlalchemy`, for example. Queries can be built and
combined with ``&`` and ``|``.

There are methods on :class:`awstin.dynamodb.Attr` and
:class:`awstin.dynamodb.Key` corresponding to the condition operations provided
by :mod:`boto3`/DynamoDB, with arithmetic comparisons exposed Pythonically.

.. toctree::
   :maxdepth: 1

   query
   query_hash_and_sort
   scan

