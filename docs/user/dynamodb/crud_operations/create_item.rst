=================
Create a New Item
=================

As demonstrated in the last section, new items are added by instantiating the
data model classes and passing them to :meth:`awstin.dynamodb.Table.put_item`.

.. literalinclude:: ../../../../examples/aws_movie_example/3_1_create_item.py
   :language: Python
