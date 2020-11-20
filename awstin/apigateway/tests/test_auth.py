import unittest

from awstin.apigateway.auth import AuthResponse, auth_handler
from awstin.awslambda import LambdaEvent


class MyAuthEvent(LambdaEvent):
    def __init__(self, token, resource_arn, principal_id):
        self.token = token
        self.resource_arn = resource_arn
        self.principal_id = principal_id

    @classmethod
    def _from_event(cls, event, _context):
        token = event["headers"]["AuthToken"]
        resource_arn = event["methodArn"]
        principal_id = event["requestContext"]["connectionId"]

        return cls(token, resource_arn, principal_id)


@auth_handler(MyAuthEvent)
def token_auth(auth_event):
    if auth_event.token == "good token":
        return AuthResponse.ACCEPT
    elif auth_event.token == "bad token":
        return AuthResponse.REJECT
    elif auth_event.token == "unauthorized token":
        return AuthResponse.UNAUTHORIZED
    elif auth_event.token == "invalid token":
        return AuthResponse.INVALID
    else:
        return "Another response"


class TestAuthHandler(unittest.TestCase):
    def test_auth_handler_accept(self):
        event = dict(
            headers={"AuthToken": "good token"},
            methodArn="arn123",
            requestContext={"connectionId": "id456"},
        )
        context = {}

        # Invoke the lambda
        result = token_auth(event, context)

        expected_result = {
            "principalId": "id456",
            "policyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": [
                            "execute-api:Invoke",
                        ],
                        "Effect": "Allow",
                        "Resource": ["arn123"],
                    }
                ]
            },
        }
        self.assertEqual(result, expected_result)

    def test_auth_handler_reject(self):
        event = dict(
            headers={"AuthToken": "bad token"},
            methodArn="arn123",
            requestContext={"connectionId": "id456"},
        )
        context = {}

        # Invoke the lambda
        result = token_auth(event, context)

        expected_result = {
            "principalId": "id456",
            "policyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": [
                            "execute-api:Invoke",
                        ],
                        "Effect": "Deny",
                        "Resource": ["arn123"],
                    }
                ]
            },
        }
        self.assertEqual(result, expected_result)

    def test_auth_handler_unauthorized(self):
        event = dict(
            headers={"AuthToken": "unauthorized token"},
            methodArn="arn123",
            requestContext={"connectionId": "id456"},
        )
        context = {}

        # Invoke the lambda
        result = token_auth(event, context)

        expected_result = {
            'statusCode': 401,
            'body': 'Unauthorized',
        }
        self.assertEqual(result, expected_result)

    def test_auth_handler_invalid(self):
        event = dict(
            headers={"AuthToken": "invalid token"},
            methodArn="arn123",
            requestContext={"connectionId": "id456"},
        )
        context = {}

        # Invoke the lambda
        result = token_auth(event, context)

        expected_result = {
            'statusCode': 500,
            'body': 'Invalid',
        }
        self.assertEqual(result, expected_result)

    def test_auth_handler_invalid_other_value(self):
        event = dict(
            headers={"AuthToken": "unexpected value"},
            methodArn="arn123",
            requestContext={"connectionId": "id456"},
        )
        context = {}

        # Invoke the lambda
        result = token_auth(event, context)

        expected_result = {
            'statusCode': 500,
            'body': 'Invalid',
        }
        self.assertEqual(result, expected_result)

    def test_auth_handler_invalid_if_data_model_lacks_attribs(self):
        class BadAuthEvent(LambdaEvent):
            token = "good token"

            @classmethod
            def _from_event(cls, _event, _context):
                return cls()

        @auth_handler(BadAuthEvent)
        def always_ok_auth(auth_event):
            return AuthResponse.ACCEPT

        result = always_ok_auth({}, {})

        expected_result = {
            'statusCode': 500,
            'body': 'Invalid',
        }
        self.assertEqual(result, expected_result)
