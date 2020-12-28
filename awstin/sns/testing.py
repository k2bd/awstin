import contextlib
import json
import os
import pkg_resources
import tempfile
import uuid
import zipfile

import boto3

from awstin.config import aws_config
from awstin.constants import TEST_LAMBDA_ENDPOINT, TEST_S3_ENDPOINT, TEST_SNS_ENDPOINT

SNS_LAMBDA_FILE = pkg_resources.resource_filename(
    "awstin.sns.testing",
    "data/sns_testing_lambda.py"
)


class SnsMessages:
    def __init__(self, filename):
        self.filename = filename
        self.messages = []

    def __iter__(self):
        if not os.path.exists(self.filename):
            # If, perhaps, the file has been deleted
            return self.messages
        else:
            with open(self.filename, "r", encoding="utf-8") as f:
                raw_messages = f.readlines()
            self.messages = [
                json.loads(message) for message in raw_messages
            ]
        yield from self.messages


@contextlib.contextmanager
def catch_sns_topic_messages(sns_topic):
    """
    Context manager that creates a subscription to the given SNS topic,
    collecting published messages into a list that's yielded.

    Uses ``responses`` to set up the subscription via http.

    Parameters
    ----------
    sns_topic : SNSTopic
        The topic to collect messages on

    Yields
    ------
    SnsMessages
        Iterable of messages published on the topic within in the context
        manager.
    """
    s3_config = aws_config(
        endpoint=os.environ.get(TEST_S3_ENDPOINT)
    )
    s3_client = boto3.client("s3", **s3_config)
    bucket_name = str(uuid.uuid4())
    s3_client.create_bucket(Bucket=bucket_name)

    lambda_config = aws_config(
        endpoint=os.environ.get(TEST_LAMBDA_ENDPOINT)
    )
    lambda_client = boto3.client("lambda", **lambda_config)

    with tempfile.TemporaryDirectory() as directory:
        messages_file = os.path.join(directory, "messages")
        deployment_package = os.path.join(directory, "lambda.zip")

        archive = zipfile.ZipFile(deployment_package, "w")
        archive.write(SNS_LAMBDA_FILE)

        s3_client.put_object(Body=archive, Bucket=bucket_name, Key='lambda.zip')

        lambda_client.create_function(
            Code={
                'S3Bucket': bucket_name,
                'S3Key': 'lambda.zip',
            },
            Description='Catch SNS topic messages.',
            FunctionName=str(uuid.uuid4()),
            Handler='sns_testing_lambda.handler',
            Publish=True,
            Role='arn:aws:iam::123456789012:role/lambda-role',
            Runtime='python3.6',
        )

        subscription = sns_topic.topic.subscribe(
            "TODO",
            ReturnSubscriptionArn=True,
        )

        messages = SnsMessages(messages_file)

        try:
            yield messages
        finally:
            # Ensure all messages have been collected by invoking the iterator
            for _ in messages:
                pass

            # Delete bucket
            bucket = s3_client.bucket(bucket_name)
            for key in bucket.objects.all():
                key.delete()
            bucket.delete()

            # Delete SNS subscription
            subscription.delete()


class SNSCleanupMixin:
    """
    unittest.TestCase mixin to cleanup all SNS topics

    Attributes
    ----------
    sns_client : boto3 client
        A SNS client
    """
    def setUp(self):
        config = aws_config(endpoint=os.environ.get(TEST_SNS_ENDPOINT))
        self.sns_client = boto3.client("sns", **config)

        self.addCleanup(self.cleanup_topics)

    def cleanup_topics(self):
        topics = self.sns_client.list_topics()["Topics"]
        arns = [topic["TopicArn"] for topic in topics]
        for arn in arns:
            self.sns_client.delete_topic(TopicArn=arn)
