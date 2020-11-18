import os

import boto3

from awstin.config import aws_config
from awstin.constants import TEST_DYNAMODB_ENDPOINT


class DynamoDB:
    """
    A client for typical use of DynamoDB.
    """
    def __init__(self, timeout=5.0, max_retries=3):
        """
        Parameters
        ----------
        timeout : float, optional
            Timeout for establishing a conneciton to DynamoDB (default 5.0)
        max_retries : int, optional
            Max retries for establishing a connection to DynamoDB (default 3)

        Raises
        ------
        EnvironmentError
            If neither the TEST_DYNAMODB_ENDPOINT or AWS_REGION environment
            variables are set
        """
        self.timeout = timeout
        self.max_retries = max_retries

        test_endpoint = os.environ.get(TEST_DYNAMODB_ENDPOINT)
        self.config = aws_config(
            timeout=timeout,
            max_retries=max_retries,
            endpoint=test_endpoint,
        )
        self.client = boto3.client('dynamodb', **self.config)
        self.resource = boto3.resource('dynamodb', **self.config)

    @property
    def tables(self):
        response = self.client.list_tables()
        tables = response["TableNames"]

        while "LastEvaluatedTableName" in response:
            response = self.client.list_tables(
                ExclusiveStartTableName=response["LastEvaluatedTableName"]
            )
            tables.extend(response["TableNames"])

        return tables

    def __getitem__(self, key):
        """
        Indexed access to DynamoDB tables.

        Returns
        -------
        Table
            the dynamodb table, if it exists.
        """
        if key in self.tables:
            return Table(self, key)
        else:
            raise KeyError(f"Table {key} does not exist")


class Table:
    """
    Interface to a DynamoDB table
    """
    def __init__(self, dynamodb_client, table_name):
        """
        Paramters
        ---------
        client : DynamoDB
            DynamoDB client
        table_name : DynamoDB Table
            Table to convert into an awstin Table
        """
        self.name = table_name

        self._dynamodb = dynamodb_client
        self._boto3_table = dynamodb_client.resource.Table(table_name)

    def _get_primary_key(self, key):
        if isinstance(key, dict):
            primary_key = key
        else:
            table_description = self._dynamodb.client.describe_table(
                TableName=self.name,
            )
            partition_key, = [
                entry["AttributeName"]
                for entry in table_description["Table"]["KeySchema"]
                if entry["KeyType"] == "HASH"
            ]
            primary_key = {partition_key: key}
        return primary_key

    def __getitem__(self, key):
        """
        Get an item, given either a primary key as a dict, or given simply the
        value of the partition key if there is no sort key
        """
        primary_key = self._get_primary_key(key)
        return self._boto3_table.get_item(Key=primary_key)["Item"]

    def put_item(self, item):
        """
        Direct exposure of put_item
        """
        return self._boto3_table.put_item(Item=item)

    def update_item(self, *args, **kwargs):
        raise NotImplementedError  # TODO

    def delete_item(self, key):
        """
        Delete an item, given either a primary key as a dict, or given simply
        the value of the partition key if there is no sort key
        """
        primary_key = self._get_primary_key(key)
        self._boto3_table.delete_item(Key=primary_key)

    # TODO: Batch Update
