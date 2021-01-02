==============
Query the Data
==============

:meth:`awstin.dynamodb.Table.query` takes a condition expression for the query,
and optionally a post-query scan expression (which is much more permissive).

.. literalinclude:: ../../../../examples/aws_movie_example/4_1_query.py
   :language: Python
