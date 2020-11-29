import json
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

    def publish(self, message, **attributes):
        """
        Publish a message to the topic

        Parameters
        ----------
        message : str
            The message to send
        **attributes : dict(str, Any)
            Message attributes to add to the message. Value must be castable
            into "String" (str), "String.Array" (list of str),
            "Number" (int or float), and "Binary" (bytes). If it's not
            classified, a bytes cast will be attempted.
        Returns
        -------
        str
            The message's unique ID
        """
        message_attributes = {}
        for key, val in attributes.items():
            if isinstance(val, str):
                message_attributes[key] = {
                    "DataType": "String",
                    "StringValue": val,
                }
            elif isinstance(val, list):
                message_attributes[key] = {
                    "DataType": "String.Array",
                    "StringValue": json.dumps(val),
                }
            elif isinstance(val, int) or isinstance(val, float):
                message_attributes[key] = {
                    "DataType": "Number",
                    "StringValue": str(val),
                }
            else:
                message_attributes[key] = {
                    "DataType": "Binary",
                    "BinaryValue": bytes(val),
                }

        return self.topic.publish(
            Message=message,
            MessageAttributes=message_attributes,
        )["MessageId"]
