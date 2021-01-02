import os

import boto3
from botocore.exceptions import ClientError

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

    Items can be retrieved from the table by a shorthand depending on the
    primary key. If it's only a partition key, items can be retrieved by the
    value of the partition key:

    ``my_table["hashval"]``

    If it's a partition and sort key, items can be retrived by a hashkey,
    sortkey tuple:

    ``my_table["hashval", 123]``

    Items can also be retrieved in a dict-like way:

    ``my_table[{"HashKeyName": "hashval", "SortKeyName": 123}]``

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

        Parameters
        ----------
        key : Any
            Primary key, specified as a hash key value, composite key tuple, or
            a dict
        """
        primary_key = self._get_primary_key(key)
        item = self._boto3_table.get_item(
            Key=primary_key,
            **self.data_model._dynamo_projection(),
        )["Item"]
        return self.data_model.deserialize(item)

    def put_item(self, item):
        """
        Put an item in the table

        Parameters
        ----------
        item : DynamoModel
            The item to put in the table
        """
        data = item.serialize()
        return self._boto3_table.put_item(Item=data)

    def update_item(self, key, update_expression, condition_expression=None):
        """
        Update an item in the table given an awstin update expression.

        Can optionally have a condition expression.

        Parameters
        ---------
        key : Any
            Primary key, specified as a hash key value, composite key tuple, or
            a dict
        update_expression : awstin.dynamodb.orm.UpdateOperator
            Update expression. See docs for construction.
        condition_expression : Query, optional
            Optional condition expression

        Returns
        -------
        DynamoModel or None
            Updated model, or None if the condition expression fails
        """
        boto_query = dict(
            Key=self._get_primary_key(key),
            ReturnValues="ALL_NEW",
            **update_expression.serialize(),
        )

        if condition_expression:
            boto_query["ConditionExpression"] = condition_expression

        try:
            result = self._boto3_table.update_item(**boto_query)
        except ClientError as e:
            if "ConditionalCheckFailedException" in str(e):
                return None
            else:
                raise e

        return self.data_model.deserialize(result["Attributes"])

    def delete_item(self, key, condition_expression=None):
        """
        Delete an item, given either a primary key as a dict, or given simply
        the value of the partition key if there is no sort key

        Parameters
        ----------
        key : Any
            Primary key of the entry to delete, specified as a hash key value,
            composite key tuple, or a dict
        condition_expression : Query, optional
            Optional condition expression for the delete, intended to make the
            operation idempotent

        Returns
        -------
        deleted : bool
            True if the delete, False if the condition was not satisfied

        Raises
        ------
        botocore.exceptions.ClientError
            If there's an error in the request.
        """
        primary_key = self._get_primary_key(key)
        condition_kwargs = (
            {"ConditionExpression": condition_expression}
            if condition_expression is not None
            else {}
        )

        try:
            self._boto3_table.delete_item(
                Key=primary_key,
                **condition_kwargs,
            )
            return True
        except ClientError as e:
            if "ConditionalCheckFailedException" in str(e):
                return False
            else:
                raise e

    def scan(self, scan_filter=None):
        """
        Yield items in from the table, optionally matching the given filter
        expression. Lazily paginates items internally.

        Parameters
        ----------
        scan_filter : Query
            An optional query constructed with awstin's query framework

        Yields
        ------
        item : DynamoModel
            An item in the table matching the filter
        """
        filter_kwargs = {}

        if scan_filter is not None:
            filter_kwargs["FilterExpression"] = scan_filter

        results = self._boto3_table.scan(
            **filter_kwargs,
            **self.data_model._get_kwargs(),
        )
        items = [self.data_model.deserialize(item) for item in results["Items"]]
        yield from items

        while "LastEvaluatedKey" in results:
            results = self._boto3_table.scan(
                ExclusiveStartKey=results["LastEvaluatedKey"],
                **filter_kwargs,
                **self.data_model._get_kwargs(),
            )
            items = [self.data_model.deserialize(item) for item in results["Items"]]
            yield from items

    def query(self, query_expression, filter_expression=None):
        """
        Yield items from the table matching some query expression and optional
        filter expression. Lazily paginates items internally.

        Parameters
        ----------
        query_expression : Query
            A Key query constructed with awstin's query syntax
        filter_expression : Query
            An additional post-query filter expression constructed with
            awstin's query syntax

        Yields
        ------
        item : DynamoModel
            An item in the table matching thw query
        """
        query_kwargs = {}

        query_kwargs["KeyConditionExpression"] = query_expression
        if filter_expression is not None:
            query_kwargs["FilterExpression"] = filter_expression

        results = self._boto3_table.query(
            **query_kwargs,
            **self.data_model._get_kwargs(),
        )
        items = [self.data_model.deserialize(item) for item in results["Items"]]
        yield from items

        while "LastEvaluatedKey" in results:
            results = self._boto3_table.query(
                ExclusiveStartKey=results["LastEvaluatedKey"],
                **query_kwargs,
                **self.data_model._get_kwargs(),
            )
            items = [self.data_model.deserialize(item) for item in results["Items"]]
            yield from items
