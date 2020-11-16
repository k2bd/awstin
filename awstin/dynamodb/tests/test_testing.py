import unittest.mock as mock
import unittest

from awstin.dynamodb.api import __name__ as DYNAMODB_NAME, DynamoDB
from awstin.dynamodb.testing import (
    temporary_dynamodb_table,
)


class TestTemporaryDynamoDBTable(unittest.TestCase):

    def test_create_dynamodb_table(self):
        dynamodb = DynamoDB()

        self.assertNotIn(
            "test_table_name",
            dynamodb.tables
        )

        with temporary_dynamodb_table("test_table_name", "test_hashkey_name"):
            self.assertIn(
                "test_table_name",
                dynamodb.tables
            )

        self.assertNotIn(
            "test_table_name",
            dynamodb.tables
        )

    def test_create_dynamodb_table_fails(self):
        fake_client = mock.Mock()

        # Mock the dynamodb client's list_tables method to return an empty
        # table names list, which should cause the loop to time out
        def fake_list_tables(): return {"TableNames": []}
        fake_client.list_tables = fake_list_tables

        mock_dynamodb = mock.patch(
            DYNAMODB_NAME + ".boto3.client",
            return_value=fake_client,
        )

        temp_table_ctx = temporary_dynamodb_table(
            "test_table_name",
            "test_hashkey_name",
            delay=0.1,
            max_attempts=1,
        )

        with mock_dynamodb:
            with self.assertRaises(RuntimeError) as err:
                with temp_table_ctx:
                    # Cannot create a table so we'll have an exception
                    pass

        self.assertEqual(
            "Could not create table 'test_table_name'",
            str(err.exception)
        )
