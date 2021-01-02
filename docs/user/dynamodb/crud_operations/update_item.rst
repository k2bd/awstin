==============
Update an Item
==============

:mod:`awstin` provides an update expression syntax that allows update
expressions to be built and chained together with ``&``. The
:meth:`awstin.dynamodb.Attr.set`, :meth:`awstin.dynamodb.Attr.remove`,
:meth:`awstin.dynamodb.Attr.add`, and :meth:`awstin.dynamodb.Attr.delete`
methods correspond to the update operations available in DynamoDB.

:meth:`awstin.dynamodb.Table.update_item` takes the primary key as the first
argument.

.. literalinclude:: ../../../../examples/aws_movie_example/3_3_update_item.py
   :language: Python
