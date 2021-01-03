=====================
Serverless Websockets
=====================

Websocket pushes can be performed with a callback URL and message:

.. code-block:: python

    from awstin.apigateway.websocket import Websocket


    Websocket("endpoint_url", stage="dev").send("callback_url", "message")
