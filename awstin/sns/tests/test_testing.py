import unittest

from awstin.sns import SNSTopic
from awstin.sns.testing import catch_sns_topic_messages, SNSCleanupMixin


class TestCatchSNSTopicMessages(unittest.TestCase, SNSCleanupMixin):
    def setUp(self):
        SNSCleanupMixin.setUp(self)

    def test_catch_sns_topic_messages(self):
        topic = SNSTopic("an-sns-topic")

        with catch_sns_topic_messages(topic) as messages:
            topic.publish("a message!")

        print(messages)
