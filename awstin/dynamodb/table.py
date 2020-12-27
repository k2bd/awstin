import os

import boto3

from awstin.config import aws_config
from awstin.constants import TEST_DYNAMODB_ENDPOINT
from awstin.dynamodb.utils import to_decimal

# Testing parameter to change table listing page size
_PAGE_SIZE = 100


class DynamoDB:
    """
    A client for use of DynamoDB via awstin.

    Tables are accessed via data models. See documentation for details.
    """

    def __init__(self, timeout=5.0, max_retries=3):
        """
        Parameters
        ----------
        timeout : float, optional
            Timeout for establishing a connection to DynamoDB (default 5.0)
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
        self.client = boto3.client("dynamodb", **self.config)
        self.resource = boto3.resource("dynamodb", **self.config)

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
                ExclusiveStartTableName=response["LastEvaluatedTableName"],
            )
            tables.extend(response["TableNames"])

        return tables

    def __getitem__(self, data_model):
        """
        Indexed access to DynamoDB tables via Python data models.

        Returns
        -------
        Table
            the dynamodb table, if it exists.
        """
        return Table(self, data_model)


class Table:
    """
    Interface to a DynamoDB table.

    Items can be retrieved in a dict-like way:
    ```
    DynamoDB()[TableModel][{"HashKeyName": "hashval", "SortKeyName": 123}]
    ```

    Items can also be retrieved from the table by a shorthand depending on the
    primary key. If it's only a partition key, items can be retrieved by the
    value of the partition key:
    ```
    DynamoDB()[TableModel]["hashval"]
    ```
    If it's a partition and sort key, items can be retrived by a hashkey,
    sortkey tuple:
    ```
    DynamoDB()[TableModel][("hashval", 123)]
    ```
    """

    def __init__(self, dynamodb_client, data_model):
        """
        Paramters
        ---------
        client : DynamoDB
            DynamoDB client
        data_model : DynamoDB Table
            Data model for interfacing with the table's contents
        """
        self.data_model = data_model
        self.name = data_model._table_name_

        self._dynamodb = dynamodb_client
        self._boto3_table = dynamodb_client.resource.Table(self.name)

    def _get_primary_key(self, key):
        if isinstance(key, dict):
            key = {k: to_decimal(v) for k, v in key.items()}
            primary_key = key
        else:
            table_description = self._dynamodb.client.describe_table(
                TableName=self.name,
            )
            (partition_key,) = [
                entry["AttributeName"]
                for entry in table_description["Table"]["KeySchema"]
                if entry["KeyType"] == "HASH"
            ]
            if isinstance(key, tuple):
                (sort_key,) = [
                    entry["AttributeName"]
                    for entry in table_description["Table"]["KeySchema"]
                    if entry["KeyType"] == "RANGE"
                ]
                primary_key = {
                    partition_key: to_decimal(key[0]),
                    sort_key: to_decimal(key[1]),
                }
            else:
                primary_key = {partition_key: to_decimal(key)}
        return primary_key

    def __getitem__(self, key):
        """
        Get an item, given either a primary key as a dict, or given simply the
        value of the partition key if there is no sort key
        """
        primary_key = self._get_primary_key(key)
        item = self._boto3_table.get_item(Key=primary_key)["Item"]
        return self.data_model._from_dynamodb(item)

    def put_item(self, item):
        """
        Direct exposure of put_item
        """
        data = item._to_dynamodb()
        return self._boto3_table.put_item(Item=data)

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

    def scan(self, scan_filter=None):
        """
        Generate all items in the table, one at a time.

        Lazily queries for more items if needed.

        Parameters
        ----------
        scan_filter : Query
            An optional query constructed with awstin's query framework

        Yields
        ------
        DynamoModel
            an item in the table
        """
        filter_kwargs = {}

        if scan_filter is not None:
            filter_kwargs["FilterExpression"] = scan_filter

        results = self._boto3_table.scan(**filter_kwargs)
        items = [self.data_model._from_dynamodb(item) for item in results["Items"]]
        yield from items

        while "LastEvaluatedKey" in results:
            results = self._boto3_table.scan(
                ExclusiveStartKey=results["LastEvaluatedKey"],
                **filter_kwargs,
            )
            items = [self.data_model._from_dynamodb(item) for item in results["Items"]]
            yield from items

    def query(self, query_expression, filter_expression=None):
        query_kwargs = {}

        query_kwargs["KeyConditionExpression"] = query_expression
        if filter_expression is not None:
            query_kwargs["FilterExpression"] = filter_expression

        results = self._boto3_table.query(**query_kwargs)
        items = [self.data_model._from_dynamodb(item) for item in results["Items"]]
        yield from items

        while "LastEvaluatedKey" in results:
            results = self._boto3_table.query(
                ExclusiveStartKey=results["LastEvaluatedKey"],
                **query_kwargs,
            )
            items = [self.data_model._from_dynamodb(item) for item in results["Items"]]
            yield from items

    # TODO: Batch Update
