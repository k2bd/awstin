import unittest.mock as mock
import unittest

from awstin.dynamodb import DynamoDB
from awstin.testing.dynamodb import (
    __name__ as TESTING_NAME,
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
        # Mock the dynamodb client's list_tables method to return an empty
        # table names list, which should cause the loop to time out
        mock_dynamodb = mock.patch(
            TESTING_NAME + ".DynamoDB.tables",
            return_value=[],
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
