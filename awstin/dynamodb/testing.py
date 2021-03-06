import contextlib
import warnings

from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from awstin.dynamodb import DynamoDB


@contextlib.contextmanager
def _temporary_table(
    table_name,
    delay,
    max_attempts,
    **table_kwargs,
):
    dynamodb = DynamoDB()

    dynamodb.client.create_table(TableName=table_name, **table_kwargs)

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
        yield
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

    Ensures that the table is created and destroyed before entering and exiting
    the context.

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
        Additional attribute definitions (boto3 specification)
    **extra_kwargs : dict
        Additional keyword arguments to pass to create_table
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

    with _temporary_table(
        table_name,
        delay,
        max_attempts,
        AttributeDefinitions=attribute_definitions,
        KeySchema=key_schema,
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
        **extra_kwargs,
    ):
        yield dynamodb[data_model]


@contextlib.contextmanager
def create_serverless_tables(
    sls_filename: str,
    # opts: Optional[Dict[str, Any]] = None,
    delay: float = 5.0,
    max_attempts: int = 10,
):
    """
    Parse a serverless.yml file, deploying any tables found in the resources
    section.

    This is currently very basic functionality that needs fleshing out.
    See k2bd/awstin#99

    Parameters
    ----------
    sls_filename : str
        Location of the serverless.yml file
    delay : float, optional
        Delay in seconds between checks if the table exists
    max_attempts : int, optional
        Max number of attempts to check if the table exists, after which the
        client gives up
    """
    with open(sls_filename, "r") as f:
        sls = load(f, Loader=Loader)

    with contextlib.ExitStack() as stack:
        for name, resource in sls["resources"]["Resources"].items():
            if resource["Type"] == "AWS::DynamoDB::Table":
                table_properties = resource["Properties"]
                table_name = table_properties.pop("TableName")
                manager = _temporary_table(
                    table_name,
                    delay=delay,
                    max_attempts=max_attempts,
                    **table_properties,
                )
                stack.enter_context(manager)
        yield
