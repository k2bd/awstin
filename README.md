# awstin

[![PyPI](https://img.shields.io/pypi/v/awstin)](https://pypi.org/project/awstin/)

![CI Build](https://github.com/k2bd/awstin/workflows/CI/badge.svg) [![codecov](https://codecov.io/gh/k2bd/awstin/branch/master/graph/badge.svg)](https://codecov.io/gh/k2bd/awstin)

Utilities for building and testing AWS applications in Python.

## Lambdas

### Production

Lambda events can be given data models that are created from the event and context:
```python
from awstin.awslambda import LambdaEvent


class CustomEvent(LambdaEvent):
    def __init__(self, request_id, memory_limit):
        self.request_id = request_id
        self.memory_limit = memory_limit

    @classmethod
    def _from_event(cls, event, context):
        request_id = event["requestContext"]["requestId"]
        memory_limit = context["memory_limit_in_mb"]
        return cls(request_id, memory_limit)
```

Lambda handlers can then be created using the `lambda_handler` decorator factory, which takes as an input the data model for the lambda event.
```python
from awstin.awslambda import lambda_handler


@lambda_handler(CustomEvent)
def handle_custom_event(event):
    print(event.request_id)
    print(event.memory_limit)
```

## API Gateway

### Production

#### Authorization Lambdas

A special version of `lambda_handler` is available for authorization lambdas. The `LambdaEvent` data model must have `resource_arn` and `principal_id` attributes, or a 500 invalid error will be returned. These return one of `AuthResponse.ACCEPT`, `AuthResponse.REJECT`, `AuthResponse.UNAUTHORIZED`, or `AuthResponse.INVALID`. `awstin` then formats the result properly.

```python
from awstin.apigateway.auth import auth_handler
from awstin.awslambda import LambdaEvent


class TokenAuthEvent(LambdaEvent):
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


@auth_handler(TokenAuthEvent)
def token_auth(auth_event):
    if auth_event.token == "good token":
        return AuthResponse.ACCEPT
    elif auth_event.token == "bad token":
        return AuthResponse.REJECT
    elif auth_event.token == "unauthorized token":
        return AuthResponse.UNAUTHORIZED
    else:
        return AuthResponse.INVALID
```


## DynamoDB

### Production

The `DynamoDB` class currently gives dict-like access to boto3 `Table`s and their contents.
This requires either the `TEST_DYNAMODB_ENDPOINT` (for integration testing) or `AWS_REGION` (for production) endpoints to be set.

```python
from awstin.dynamodb import DynamoDB


dynamodb = DynamoDB()

# List of available tables
tables = dynamodb.tables

# Access a single table
table = dynamodb["my_table"]

# Tables that only have a partition key can be accessed directly by their
# partition key
item = table["an_item"]

# Full primary key access is also available
item = table[{"primary_key_name": "an_item"}]
```

### Testing

For integration testing, a context manager to create and then tear-down a DynamoDB table is provided. More flexible control over the created table is planned.

```python
from awstin.dynamodb.testing import temporary_dynamodb_table


with temporary_dynamodb_table("table_name", "hashkey_name") as table:
    item = {
        "hashkey_name": "test_value",
        "another_key": 5,
    }
    table.put_item(item)
```


## SNS

SNS topics can be retrieved by name and published to with the message directly:

```python
from awstin.sns import SNSTopic


topic = SNSTopic("topic-name")
message_id = topic.publish("a message")
```
