import unittest

from awstin.apigateway import auth
from awstin.awslambda import lambda_handler


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
