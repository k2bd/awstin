import os

import boto3

from awstin.config import aws_config
from awstin.constants import TEST_SNS_ENDPOINT


class SNSTopic:
    """
    A client for typical use of an SNS topic
    """
    def __init__(self, topic_name):
        """
        Parameters
        ----------
        topic_name : str
            Name of the topic
        """
        config = aws_config(endpoint=os.environ.get(TEST_SNS_ENDPOINT))
        self.sns = boto3.resource("sns", **config)
        self.topic = self.sns.create_topic(Name=topic_name)

    def publish(self, message):
        """
        Publish a message to the topic

        Parameters
        ----------
        message : str
            The message to send

        Returns
        -------
        str
            The message's unique ID
        """
        return self.topic.publish(Message=message)["MessageId"]
