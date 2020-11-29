# awstin

[![PyPI](https://img.shields.io/pypi/v/awstin)](https://pypi.org/project/awstin/) ![Dev Status](https://img.shields.io/pypi/status/awstin)

![CI Build](https://github.com/k2bd/awstin/workflows/CI/badge.svg) [![codecov](https://codecov.io/gh/k2bd/awstin/branch/master/graph/badge.svg)](https://codecov.io/gh/k2bd/awstin)

High-level utilities for building and testing AWS applications in Python.


## DynamoDB

### Production

The `DynamoDB` class currently gives dict-like access to boto3 `Table`s and their contents.
This requires either the `TEST_DYNAMODB_ENDPOINT` (for integration testing) or `AWS_REGION` (for production) environment variable to be set.

```python
from awstin.dynamodb import DynamoDB


dynamodb = DynamoDB()

# List of available tables
tables = dynamodb.list_tables()

# Access a table by name
table1 = dynamodb["my_table"]

# Tables that only have a partition key can be accessed directly by their
# partition key
item1 = table1["an_item"]

# Tables that have partition and sort keys can be accessed by a tuple
table2 = dynamodb["another_table"]
item2 = table2[("hashval", 123)]

# Full primary key access is also available
item3 = table2[{"hashkey_name": "hashval", "sortkey_name": 123}]
```

Tables can be scanned without worrying about pagination. `Table.scan` yields items, requesting another page of items lazily only when it's out of items in a page.


### Testing

For integration testing, a context manager to create and then automatically tear-down a DynamoDB table is provided.
The context manager waits for the table to be created/deleted before entering/exiting to avoid testing issues.
Hashkey and sortkey info can be provided.

```python
from awstin.dynamodb.testing import temporary_dynamodb_table


with temporary_dynamodb_table("table_name", "hashkey_name") as table:
    item = {
        "hashkey_name": "test_value",
        "another_key": 5,
    }
    table.put_item(item)
```


## Lambdas

### Production

Lambda handlers can be made more readable by separating event parsing from business logic.
The `lambda_handler` decorator factory takes a parser for the triggering event and context, and returns individual values to be used in the wrapped function.
```python
from awstin.awslambda import lambda_handler

def event_parser(event, context):
    request_id = event["requestContext"]["requestId"]
    memory_limit = context["memory_limit_in_mb"]
    return request_id, memory_limit


@lambda_handler(event_parser)
def handle_custom_event(request_id, memory_limit):
    print(request_id)
    print(memory_limit)
```


## API Gateway

### Authorization Lambdas

#### Production

Authorizor lambda responses can be generated with helper functions provided by `awstin.apigateway.auth`. `accept`, `reject`, `unauthorized`, and `invalid` will produce properly formatted auth lambda responses.

```python
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
```

### Websockets

#### Production

Websocket pushes can be performed with a callback URL and message:

```python
from awstin.apigateway.websocket import Websocket


Websocket("endpoint_url", "dev").send("callback_url", "message")
```


## SNS

### Production

SNS topics can be retrieved by name and published to with the message directly.
This requires either the `TEST_SNS_ENDPOINT` (for integration testing) or `AWS_REGION` (for production) environment variable to be set.

```python
from awstin.sns import SNSTopic


topic = SNSTopic("topic-name")
message_id = topic.publish("a message")
```

Message attributes can be set from the kwargs of the publish:

```python
topic.publish(
    "another message",
    attrib_a="a string",
    attrib_b=1234,
    attrib_c=["a", "b", False, None],
    attrib_d=b"bytes value",
)
```
