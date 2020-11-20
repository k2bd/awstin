import logging
from enum import Enum, auto

logger = logging.getLogger(__name__)


class AuthResponse(Enum):
    ACCEPT = auto()
    REJECT = auto()
    UNAUTHORIZED = auto()
    INVALID = auto()


def auth_handler(event_type):
    """
    Decorates a lambda handler that takes an object of ``event_type`` and
    returns one of the ``AuthResponse`` values. ``event_type`` must have
    ``principle_id`` and ``resource_arn`` attributes to create the auth
    response.

    A corresponding authorization response is created:

    AuthResonse.ACCEPT:
        Allow access
    AuthResponse.REJECT:
        Reject access
    AuthResponse.UNAUTHORIZED:
        Return a 401 unauthorized
    AuthResponse.INVALID, or any other value:
        Return a 500 invalid

    Parameters
    ----------
    event_type : LambdaEvent
        The type of event the handler is handling. The handler should accept
        this type of object.
    """

    def handler(func):
        def wrapped(event, context):
            logger.info(f"Event: {event!r}")

            parsed = event_type.from_event(event, context)

            try:
                principal_id = parsed.principal_id
                resource_arn = parsed.resource_arn
            except AttributeError:
                return _invalid_response()

            result = func(parsed)

            logger.info(f"Result: {result!r}")

            if result == AuthResponse.ACCEPT:
                return _accept_response(principal_id, resource_arn)
            elif result == AuthResponse.REJECT:
                return _reject_response(principal_id, resource_arn)
            elif result == AuthResponse.UNAUTHORIZED:
                return _unauthorized_response()
            else:
                return _invalid_response()

        return wrapped
    return handler


def _auth_response(principal_id, resource_arn, effect):
    return {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": [
                        "execute-api:Invoke",
                    ],
                    "Effect": effect,
                    "Resource": [resource_arn],
                }
            ]
        },
    }


def _accept_response(principal_id, resource_arn):
    return _auth_response(principal_id, resource_arn, "Allow")


def _reject_response(principal_id, resource_arn):
    return _auth_response(principal_id, resource_arn, "Deny")


def _unauthorized_response():
    return {
        'statusCode': 401,
        'body': 'Unauthorized',
    }


def _invalid_response():
    return {
        'statusCode': 500,
        'body': 'Invalid',
    }
