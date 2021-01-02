===================
Loading Sample Data
===================

:meth:`awstin.dynamodb.Table.put_item` takes instances of the data model
to insert into DynamoDB after serialization.

:mod:`awstin` handles conversions between ``float`` and ``Decimal`` internally,
so ``float`` can be used when instantiating data models.

.. literalinclude:: ../../../../examples/aws_movie_example/2_1_load_data_a.py
   :language: Python

:class:`awstin.dynamodb.DynamoModel` has
:meth:`awstin.dynamodb.DynamoModel.serialize` and
:meth:`awstin.dynamodb.DynamoModel.deserialize` methods to convert between
DynamoDB representations of data and the data models, and can be used to read
data from JSON.

.. literalinclude:: ../../../../examples/aws_movie_example/2_1_load_data_b.py
   :language: Python
