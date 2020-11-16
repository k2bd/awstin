import os

import boto3
from botocore.client import Config

from awstin.constants import AWS_REGION, TEST_DYNAMODB_ENDPOINT


def _dynamodb_config(timeout=5.0, max_retries=3):
    """
    Create kwargs used to create a DynamoDB client or resource.

    Parameters
    ----------
    timeout : float, optional
        Timeout for establishing a conneciton to DynamoDB (default 5.0)
    max_retries : int, optional
        Max retries for establishing a connection to DynamoDB (default 3)

    Returns
    -------
    dict
        kwargs to pass to boto3 client or resource to get the DynamoDB table

    Raises
    ------
    EnvironmentError
        If neither the TEST_DYNAMODB_ENDPOINT or AWS_REGION environment
        variables are set
    """
    test_endpoint = os.environ.get(TEST_DYNAMODB_ENDPOINT)
    region_name = os.environ.get(AWS_REGION)

    if test_endpoint:
        kwargs = {"endpoint_url": test_endpoint}
    elif region_name:
        kwargs = {"region_name": region_name}
    else:
        raise EnvironmentError(
            "Please set either the AWS_REGION or TEST_DYNAMODB_ENDPOINT "
            "environment variable."
        )

    config = Config(
        connect_timeout=timeout,
        retries={'max_attempts': max_retries},
    )

    return {
        "config": config,
        **kwargs
    }


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

        self.config = _dynamodb_config(
            timeout=timeout,
            max_retries=max_retries,
        )
        self.client = boto3.client('dynamodb', **self.config)
        self.resource = boto3.resource('dynamodb', **self.config)

    @property
    def tables(self):
        return self.client.list_tables()["TableNames"]

    def __getitem__(self, key):
        """
        Indexed access to DynamoDB tables.

        Returns
        -------
        Table
            the dynamodb table, if it exists.
        """
        return self.resource.Table(key)
