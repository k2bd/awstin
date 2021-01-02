====================
Defining Data Models
====================

The elemental object for DynamoDB in :mod:`awstin` is not the JSON dict, but
instead a :class:`awstin.dynamodb.DynamoModel` subclass representing a view
of the data in a table or index.

.. toctree::
   :maxdepth: 1

   dynamo_model
   access_tables
