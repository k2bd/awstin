==============================================
The :class:`awstin.dynamodb.DynamoModel` Class
==============================================

Table Models
------------

The elemental object for DynamoDB in :mod:`awstin` is not the JSON dict, but
instead a :class:`awstin.dynamodb.DynamoModel` subclass representing a view
of the data in a table or index.

Each :class:`awstin.dynamodb.DynamoModel` subclass should have a
``_table_name_`` attribute on its class definition which is the name of the
table in DynamoDB the model relates to.

The class should also have definitions of table keys and any additional
attributes you want through :class:`awstin.dynamodb.Key` and
:class:`awstin.dynamodb.Attr` definitions. By default, the attribute name on
the data model is the property name in DynamoDB, but a property name can also
be specified by passing in a string argument.

Below is a data model representing information for the Movies table in the AWS
documentation example.

.. literalinclude:: ../../../../examples/aws_movie_example/models.py
   :language: Python

Index Models
------------

:class:`awstin.dynamodb.DynamoModel` subclasses can also reference local or
global secondary indexes. These work the same as table data models, but in
addition to the ``_table_name_`` attribute, an ``_index_name_`` attribute
should also be provided, defining the name of the index.
