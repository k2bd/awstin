import os
import unittest
from unittest import mock

import boto3

from awstin.config import aws_config
from awstin.constants import TEST_SNS_ENDPOINT
from awstin.sns import SNSTopic


class TestSNSTopic(unittest.TestCase):
    def setUp(self):
        config = aws_config(endpoint=os.environ.get(TEST_SNS_ENDPOINT))
        self.sns_client = boto3.client("sns", **config)

        self.addCleanup(self.cleanup_topics)

    def cleanup_topics(self):
        topics = self.sns_client.list_topics()["Topics"]
        arns = [topic["TopicArn"] for topic in topics]
        for arn in arns:
            self.sns_client.delete_topic(TopicArn=arn)

    def test_instantiate_creates_topic(self):
        SNSTopic("name_of_topic")
        topics = self.sns_client.list_topics()["Topics"]
        self.assertEqual(len(topics), 1)
        topic, = topics
        self.assertTrue(topic["TopicArn"].endswith("name_of_topic"))

    def test_post_to_sns_topic(self):
        # Mock out SNS publish - may be possible to integration test later
        # with subscriptions to lambdas.
        topic = SNSTopic("some_topic")

        mock_sns = mock.Mock()
        mock_sns.publish = mock.Mock(return_value={"MessageId": "msg_id"})

        with mock.patch.object(topic, "topic", mock_sns):
            message_id = topic.publish("a cool message")

        mock_sns.publish.assert_called_once_with(
            Message="a cool message",
            MessageAttributes={},
        )
        self.assertEqual(message_id, "msg_id")

    def test_post_to_sns_topic_attributes(self):
        topic = SNSTopic("atopic")

        # Create a phony subsscription for boto3 reasons
        topic.topic.subscribe(Protocol="http", Endpoint="http://0")

        with mock.patch.object(topic, "topic", wraps=topic.topic) as w_topic:
            topic.publish(
                "a_cool_message",
                attrib_a="a string",
                attrib_b=1234,
                attrib_c=["a", "b", False, None],
                attrib_d=b"bytes value",
            )

        w_topic.publish.assert_called_once_with(
            Message="a_cool_message",
            MessageAttributes={
                "attrib_a": {
                    "DataType": "String",
                    "StringValue": "a string",
                },
                "attrib_b": {
                    "DataType": "Number",
                    "StringValue": "1234",
                },
                "attrib_c": {
                    "DataType": "String.Array",
                    "StringValue": '["a", "b", false, null]',
                },
                "attrib_d": {
                    "DataType": "Binary",
                    "BinaryValue": b"bytes value",
                },
            },
        )
