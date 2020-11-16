import contextlib
import os

from awstin.dynamodb import dynamodb_client


@contextlib.contextmanager
def temporary_dynamodb_table(
        table_name,
        hashkey_name,
        hashkey_type="S",
        delay=5.0,
        max_attempts=10,):
    """
    Context manager creating a temporary DynamoDB table for testing.

    Parameters
    ----------
    table_name : str
        Name of the table to create
    hashkey_name : str
        Name of the hash key of the table
    hashkey_type : str, optional
        Type of the hash key ("S", "N", or "B"). Default "S"
    delay : float, optional
        Delay in seconds between checks if the table exists
    max_attempts : int, optional
        Max number of attempts to check if the table exists, after which the
        client gives up.
    """
    dynamodb = dynamodb_client()

    dynamodb.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': hashkey_name,
                'AttributeType': hashkey_type,
            },
        ],
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': hashkey_name,
                'KeyType': 'HASH',
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123
        },
    )

    exists_waiter = dynamodb.get_waiter("table_exists")
    not_exists_waiter = dynamodb.get_waiter("table_not_exists")

    result = exists_waiter.wait(
        TableName=table_name,
        WaiterConfig={
            'Delay': delay,
            'MaxAttempts': max_attempts,
        }
    )
    if result is not None:
        raise RuntimeError("Could not create table {!r}".format(table_name))

    try:
        yield
    finally:
        dynamodb.delete_table(TableName=table_name)

        result = not_exists_waiter.wait(
            TableName=table_name,
            WaiterConfig={
                'Delay': delay,
                'MaxAttempts': max_attempts,
            }
        )
        if result is not None:
            msg = "Could not delete table {!r}"
            raise RuntimeError(msg.format(table_name))


@contextlib.contextmanager
def set_env(**env_vars):
    """
    Context manager to temporarily set environment variables. Restores previous
    values on leaving

    Parameters
    ----------
    **env_vars : Dict(str, str or None)
        Environment variables to set temporarily. If None, the environment
        variable will be tempoarily removed.
    """
    old_environ = dict(os.environ)
    for key, value in env_vars.items():
        if value is None:
            if key in os.environ:
                del os.environ[key]
        else:
            os.environ[key] = value

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)
