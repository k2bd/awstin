import contextlib
import os
import uuid
import warnings

import boto3
import responses

from awstin.config import aws_config
from awstin.constants import TEST_SNS_ENDPOINT


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
    list
        List of messages published on the topic wihin in the context manager.
    """
    # TODO: make filter more specific
    warnings.simplefilter("ignore", ResourceWarning)

    messages = []

    def _add_message(request):
        messages.append(request)
        return (200, {}, "")

    endpoint = "http://" + str(uuid.uuid4())

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add_callback(
            responses.POST,
            endpoint,
            callback=_add_message,
            content_type='application/json',
        )

        subscription = sns_topic.topic.subscribe(
            Protocol="http",
            Endpoint=endpoint,
            ReturnSubscriptionArn=True,
        )

        try:
            yield messages
        finally:
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
