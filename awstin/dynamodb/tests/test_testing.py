import unittest
import unittest.mock as mock

from awstin.dynamodb import DynamoDB
from awstin.dynamodb import __name__ as DYNAMODB_NAME
from awstin.dynamodb.testing import temporary_dynamodb_table


class TestTemporaryDynamoDBTable(unittest.TestCase):

    def test_create_dynamodb_table(self):
        dynamodb = DynamoDB()

        self.assertNotIn(
            "test_table_name",
            dynamodb.list_tables()
        )

        with temporary_dynamodb_table("test_table_name", "test_hashkey_name"):
            self.assertIn(
                "test_table_name",
                dynamodb.list_tables()
            )

        self.assertNotIn(
            "test_table_name",
            dynamodb.list_tables()
        )

    def test_create_dynamodb_table_composite_key(self):
        dynamodb = DynamoDB()

        with temporary_dynamodb_table(
            "test_table_name",
            "test_hashkey_name",
            sortkey_name="sortkey_name",
        ):
            self.assertIn(
                "test_table_name",
                dynamodb.list_tables()
            )
            table_desc = dynamodb.client.describe_table(
                TableName="test_table_name",
            )

        # Both a hash and sort key
        self.assertEqual(len(table_desc["Table"]["KeySchema"]), 2)

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
