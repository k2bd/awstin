import contextlib
import warnings

from awstin.dynamodb import DynamoDB


@contextlib.contextmanager
def temporary_dynamodb_table(
    data_model,
    hashkey_name,
    hashkey_type="S",
    sortkey_name=None,
    sortkey_type="S",
    delay=5.0,
    max_attempts=10,
    extra_attributes=None,
    **extra_kwargs,
):
    """
    Context manager creating a temporary DynamoDB table for testing.

    Parameters
    ----------
    data_model : DynamoModel
        Model to interface with this table
    hashkey_name : str
        Name of the hash key of the table
    hashkey_type : str, optional
        Type of the hash key ("S", "N", or "B"). Default "S"
    sortkey_name : str, optional
        Optional sort key for the temporary table
    sortkey_type : str, optional
        Type of the sort key if there is one ("S", "N", or "B"). Default "S"
    delay : float, optional
        Delay in seconds between checks if the table exists
    max_attempts : int, optional
        Max number of attempts to check if the table exists, after which the
        client gives up.
    extra_attributes : dict, optional
        Additional attribute definitions to add to create_table
    **extra_kwargs : dict
        Additional keyword arguments will be passed to create_table
    """
    # TODO: make filter more specific
    warnings.simplefilter("ignore", ResourceWarning)

    table_name = data_model._table_name_

    dynamodb = DynamoDB()

    attribute_definitions = [
        {
            "AttributeName": hashkey_name,
            "AttributeType": hashkey_type,
        },
    ]
    key_schema = [
        {
            "AttributeName": hashkey_name,
            "KeyType": "HASH",
        },
    ]
    if sortkey_name is not None and sortkey_type is not None:
        attribute_definitions.append(
            {
                "AttributeName": sortkey_name,
                "AttributeType": sortkey_type,
            }
        )
        key_schema.append(
            {
                "AttributeName": sortkey_name,
                "KeyType": "RANGE",
            },
        )

    if extra_attributes:
        attribute_definitions.extend(extra_attributes)

    dynamodb.client.create_table(
        TableName=table_name,
        AttributeDefinitions=attribute_definitions,
        KeySchema=key_schema,
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
        **extra_kwargs,
    )

    exists_waiter = dynamodb.client.get_waiter("table_exists")
    not_exists_waiter = dynamodb.client.get_waiter("table_not_exists")

    result = exists_waiter.wait(
        TableName=table_name,
        WaiterConfig={
            "Delay": delay,
            "MaxAttempts": max_attempts,
        },
    )
    if result is not None:
        raise RuntimeError("Could not create table {!r}".format(table_name))

    try:
        yield dynamodb[data_model]
    finally:
        dynamodb.client.delete_table(TableName=table_name)

        result = not_exists_waiter.wait(
            TableName=table_name,
            WaiterConfig={
                "Delay": delay,
                "MaxAttempts": max_attempts,
            },
        )
        if result is not None:
            msg = "Could not delete table {!r}"
            raise RuntimeError(msg.format(table_name))
