import os

import boto3

from awstin.config import aws_config
from awstin.constants import TEST_DYNAMODB_ENDPOINT

# Testing parameter to change table listing page size
_PAGE_SIZE = 100


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

    def list_tables(self):
        """
        Return a list of all table names in this DynamoDB instance.

        Returns
        -------
        list of str
            Table names
        """
        response = self.client.list_tables(Limit=_PAGE_SIZE)
        tables = response["TableNames"]

        while "LastEvaluatedTableName" in response:
            response = self.client.list_tables(
                Limit=_PAGE_SIZE,
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
        return Table(self, key)


class Table:
    """
    Interface to a DynamoDB table.

    Items can be retrieved in a dict-like way:
    ```
    Table("table_name")[{"HashKeyName": "hashval", "SortKeyName": 123}]
    ```

    Items can also be retrieved from the table by a shorthand depending on the
    primary key. If it's only a partition key, items can be retrieved by the
    value of the partition key:
    ```
    Table("table_name")["hashval"]
    ```
    If it's a partition and sort key, items can be retrived by a hashkey,
    sortkey tuple:
    ```
    Table("table_name")[("hashval", 123)]
    ```
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
            if isinstance(key, tuple):
                sort_key, = [
                    entry["AttributeName"]
                    for entry in table_description["Table"]["KeySchema"]
                    if entry["KeyType"] == "RANGE"
                ]
                primary_key = {partition_key: key[0], sort_key: key[1]}
            else:
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
        """
        For now, direct exposure of update_item
        """
        return self._boto3_table.update_item(*args, **kwargs)

    def delete_item(self, key):
        """
        Delete an item, given either a primary key as a dict, or given simply
        the value of the partition key if there is no sort key
        """
        primary_key = self._get_primary_key(key)
        self._boto3_table.delete_item(Key=primary_key)

    def scan(self):
        """
        Generate all items in the table, one at a time.

        Lazily queries for more items if needed.

        Yields
        ------
        dict
            an item in the table
        """
        # TODO: scan filters

        results = self._boto3_table.scan()
        items = results["Items"]
        yield from items

        while "LastEvaluatedKey" in results:
            results = self._boto3_table.scan(
                ExclusiveStartKey=results["LastEvaluatedKey"],
            )
            items = results["Items"]
            yield from items

    # TODO: Batch Update
