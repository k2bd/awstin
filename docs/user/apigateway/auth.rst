============
Auth Lambdas
============

Authorizor lambda responses can be generated with helper functions provided by
:mod:`awstin.apigateway.auth`. :func:`awstin.apigateway.auth.accept`,
:func:`awstin.apigateway.auth.reject`,
:func:`awstin.apigateway.auth.unauthorized`,
and :func:`awstin.apigateway.auth.invalid` will produce properly formatted auth
lambda responses.

.. code-block:: python

    from awstin.apigateway import auth


    def auth_event_parser(event, _context):
        token = event["headers"]["AuthToken"]
        resource_arn = event["methodArn"]
        principal_id = event["requestContext"]["connectionId"]

        return token, resource_arn, principal_id


    @lambda_handler(auth_event_parser)
    def token_auth(token, resource_arn, principal_id):
        if token == "good token":
            return auth.accept(principal_id, resource_arn)
        elif token == "bad token":
            return auth.reject(principal_id, resource_arn)
        elif token == "unauthorized token":
            return auth.unauthorized()
        else:
            return auth.invalid()
