==============================
Update an Item (Conditionally)
==============================

:meth:`awstin.dynamodb.Attr.set` can optionally be given a condition
expression.
The updated value will be returned as an instance of the data model. If the
update condition fails, ``None`` will be returned instead.

More information about the query/condition syntax is given in
:ref:`query-and-scan`.

.. literalinclude:: ../../../../examples/aws_movie_example/3_5_conditional_update.py
   :language: Python
