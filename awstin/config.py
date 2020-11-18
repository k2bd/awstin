import os

from botocore.client import Config

from awstin.constants import AWS_REGION


def aws_config(timeout=5.0, max_retries=3, endpoint=None):
    """
    Create kwargs used to configure a boto3 client or resource.

    Parameters
    ----------
    timeout : float, optional
        Timeout for establishing a conneciton to DynamoDB (default 5.0)
    max_retries : int, optional
        Max retries for establishing a connection to DynamoDB (default 3)
    endpoint : str or None, optional
        Endpoint for the AWS service for testing. If not provided, the
        AWS_REGION environment variable will be used to specify the AWS region

    Returns
    -------
    dict
        kwargs to pass to boto3 client or resource

    Raises
    ------
    EnvironmentError
        If the AWS_REGION environment variable is required but is not set
    """
    region_name = os.environ.get(AWS_REGION)
    if endpoint:
        kwargs = {"endpoint_url": endpoint}
    elif region_name:
        kwargs = {"region_name": region_name}
    else:
        raise EnvironmentError(
            "Please set either the AWS_REGION environment variable or set a "
            "testing endpoint."
        )

    config = Config(
        connect_timeout=timeout,
        retries={'max_attempts': max_retries},
    )

    return {
        "config": config,
        **kwargs
    }
