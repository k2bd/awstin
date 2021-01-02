==============
Delete an Item
==============

Items can also be deleted by primary key value. A condition expression can
also be provided.
If the item is deleted, :meth:`awstin.dynamodb.Table.delete_item` returns
``True``. If the condition fails, it returns ``False``.

More information about the query/condition syntax is given in
:ref:`query-and-scan`.

.. literalinclude:: ../../../../examples/aws_movie_example/3_6_delete_item.py
   :language: Python
