============
Read an Item
============

Items can be retrieved from a table in a dict-like style. If the table only has
a hash key, items are accessed from the value of the hash key. If it has a hash
key and a sort key, they're accessed by a tuple of (hash key value, sort key
value).

.. literalinclude:: ../../../../examples/aws_movie_example/3_2_read_item.py
   :language: Python
