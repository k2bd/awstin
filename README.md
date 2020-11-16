# Awstin

![CI Build](https://github.com/k2bd/awstin/workflows/CI/badge.svg) [![codecov](https://codecov.io/gh/k2bd/awstin/branch/master/graph/badge.svg)](https://codecov.io/gh/k2bd/awstin)

Utilities for building and testing AWS applications in Python.

## Lambdas

### Production

Lambda events can be given data models that are created from the event and context:
```python
from awstin.awslambda.api import LambdaEvent


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
from awstin.awslambda.api import lambda_handler


@lambda_handler(CustomEvent)
def handle_custom_event(event):
    print(event.request_id)
    print(reqeust.memory_limit)
```


## DynamoDB

### Production

The `DynamoDB` class currently gives dict-like access to boto3 `Table`s. This requires either the `TEST_DYNAMODB_ENDPOINT` (for integration testing) or `AWS_REGION` (for production) endpoints to be set.

```python
from awstin.dynamodb.api import DynamoDB


dynamodb = DynamoDB()

# List of available tables
tables = dynamodb.tables

# Access a single table
table = dynamodb["my_table"]
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
    table.put_item(Item=item)
```
