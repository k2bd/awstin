import unittest
from unittest import mock

from awstin.config import __name__ as CONFIG_NAME
from awstin.config import aws_config
from awstin.constants import AWS_REGION
from awstin.environment import set_env


class TestConfig(unittest.TestCase):
    def test_aws_config_production(self):
        # Check that in a production environment, the resource is called
        # with the right kwargs
        temp_env = {
            AWS_REGION: "SOME REGION",
        }

        with set_env(**temp_env):
            config = aws_config("test_table_name")

        self.assertEqual(config["region_name"], "SOME REGION")
        self.assertNotIn("endpoint_url", config)

    def test_aws_config_no_env_vars(self):
        temp_env = {
            AWS_REGION: None,
        }

        with set_env(**temp_env):
            with self.assertRaises(EnvironmentError) as err:
                aws_config("a table name")

        self.assertEqual(
            "Please set either the AWS_REGION environment variable or set a "
            "testing endpoint.",
            str(err.exception)
        )

    def test_aws_config_kwargs(self):
        # Check that kwargs are passed to the resource properly

        with mock.patch(CONFIG_NAME+".Config") as mock_config:
            aws_config(timeout=11, max_retries=55, endpoint="123.456")

        mock_config.assert_called_once_with(
            connect_timeout=11,
            retries={"max_attempts": 55}
        )
