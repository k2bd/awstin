import unittest.mock as mock
import os
import unittest

from awstin.dynamodb import dynamodb_client
from awstin.testing import (
    __name__ as TESTING_NAME,
    set_env,
    temporary_dynamodb_table,
)

UNLIKELY_ENV_VAR = "THIS_ENV_VAR_IS_UNLIKELY_TO_BE_IN_USE_150y71i1"


class TestSetEnv(unittest.TestCase):

    def setUp(self):
        # Always remove the used environment variable
        def reset_env_var():
            if UNLIKELY_ENV_VAR in os.environ:
                del os.environ[UNLIKELY_ENV_VAR]
        self.addCleanup(reset_env_var)

    def test_temp_set_new_env_var(self):
        self.assertNotIn(UNLIKELY_ENV_VAR, os.environ)

        temp_env = {UNLIKELY_ENV_VAR: "test value"}

        with set_env(**temp_env):
            self.assertEqual(os.environ.get(UNLIKELY_ENV_VAR), "test value")

        self.assertNotIn(UNLIKELY_ENV_VAR, os.environ)

    def test_env_var_already_in_use(self):
        os.environ[UNLIKELY_ENV_VAR] = "old value"

        temp_env = {UNLIKELY_ENV_VAR: "new value"}

        with set_env(**temp_env):
            self.assertEqual(os.environ.get(UNLIKELY_ENV_VAR), "new value")

        self.assertEqual(os.environ.get(UNLIKELY_ENV_VAR), "old value")

    def test_remove_env_var(self):
        os.environ[UNLIKELY_ENV_VAR] = "old value"

        temp_env = {UNLIKELY_ENV_VAR: None}

        with set_env(**temp_env):
            self.assertNotIn(UNLIKELY_ENV_VAR, os.environ)

        self.assertEqual(os.environ.get(UNLIKELY_ENV_VAR), "old value")

    def test_remove_nonexist_env_var(self):
        # Test setting an env var to None that wasn't previously set
        temp_env = {UNLIKELY_ENV_VAR: None}

        with set_env(**temp_env):
            self.assertNotIn(UNLIKELY_ENV_VAR, os.environ)

        self.assertNotIn(UNLIKELY_ENV_VAR, os.environ)


class TestTemporaryDynamoDBTable(unittest.TestCase):

    def test_create_dynamodb_table(self):
        dynamodb = dynamodb_client()

        self.assertNotIn(
            "test_table_name",
            dynamodb.list_tables()["TableNames"]
        )

        with temporary_dynamodb_table("test_table_name", "test_hashkey_name"):
            self.assertIn(
                "test_table_name",
                dynamodb.list_tables()["TableNames"]
            )

        self.assertNotIn(
            "test_table_name",
            dynamodb.list_tables()["TableNames"]
        )

    def test_create_dynamodb_table_fails(self):
        fake_client = mock.Mock()

        # Mock the dynamodb client's list_tables method to return an empty
        # table names list, which should cause the loop to time out
        def fake_list_tables(): return {"TableNames": []}
        fake_client.list_tables = fake_list_tables

        mock_dynamodb = mock.patch(
            TESTING_NAME + ".dynamodb_client",
            return_value=fake_client,
        )

        temp_table_ctx = temporary_dynamodb_table(
            "test_table_name",
            "test_hashkey_name",
            delay=0.1,
            max_attempts=1,
        )

        with self.assertRaises(RuntimeError) as err:
            with mock_dynamodb, temp_table_ctx:
                # Cannot create a table so we'll have an exception
                pass

        self.assertEqual(
            "Could not create table 'test_table_name'",
            str(err.exception)
        )
