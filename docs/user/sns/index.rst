===
SNS
===

SNS topics can be retrieved by name and published to with the message directly.
This requires either the ``TEST_SNS_ENDPOINT`` (for integration testing) or
``AWS_REGION`` (for production) environment variable to be set.

.. code-block:: python

   from awstin.sns import SNSTopic


   topic = SNSTopic("topic-name")
   message_id = topic.publish("a message")

Message attributes can also be set from the kwargs of the publish.

.. code-block:: python

   topic.publish(
      "another message",
      attrib_a="a string",
      attrib_b=1234,
      attrib_c=["a", "b", False, None],
      attrib_d=b"bytes value",
   )
