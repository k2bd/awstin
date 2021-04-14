import unittest
import unittest.mock as mock
import os

from awstin.dynamodb import DynamoDB, DynamoModel, Key, Attr
from awstin.dynamodb.table import __name__ as DYNAMODB_NAME
from awstin.dynamodb.testing import temporary_dynamodb_table, create_serverless_tables

SIMPLE_SERVERLESS_YML = os.path.join(os.path.dirname(__file__), "data", "simple_serverless.yml")


class TestTemporaryDynamoDBTable(unittest.TestCase):
    def test_create_dynamodb_table(self):
        dynamodb = DynamoDB()

        class Model(DynamoModel):
            _table_name_ = "test_table_name"
            test_hashkey_name = Key()

        self.assertNotIn("test_table_name", dynamodb.list_tables())

        with temporary_dynamodb_table(Model, "test_hashkey_name"):
            self.assertIn("test_table_name", dynamodb.list_tables())

        self.assertNotIn("test_table_name", dynamodb.list_tables())

    def test_create_dynamodb_table_composite_key(self):
        dynamodb = DynamoDB()

        class Model(DynamoModel):
            _table_name_ = "test_table_name"
            test_hashkey_name = Key()
            sortkey_name = Key()

        with temporary_dynamodb_table(
            Model,
            "test_hashkey_name",
            sortkey_name="sortkey_name",
        ):
            self.assertIn("test_table_name", dynamodb.list_tables())
            table_desc = dynamodb.client.describe_table(
                TableName="test_table_name",
            )

        # Both a hash and sort key
        self.assertEqual(len(table_desc["Table"]["KeySchema"]), 2)

    def test_create_dynamodb_table_fails(self):
        fake_client = mock.Mock()

        class Model(DynamoModel):
            _table_name_ = "test_table_name"
            test_hashkey_name = Key()

        # Mock the dynamodb client's list_tables method to return an empty
        # table names list, which should cause the loop to time out
        def fake_list_tables():
            return {"TableNames": []}

        fake_client.list_tables = fake_list_tables

        mock_dynamodb = mock.patch(
            DYNAMODB_NAME + ".boto3.client",
            return_value=fake_client,
        )

        temp_table_ctx = temporary_dynamodb_table(
            Model,
            "test_hashkey_name",
            delay=0.1,
            max_attempts=1,
        )

        with mock_dynamodb:
            with self.assertRaises(RuntimeError) as err:
                with temp_table_ctx:
                    # Cannot create a table so we'll have an exception
                    pass

        self.assertEqual("Could not create table 'test_table_name'", str(err.exception))


class TestCreateServerlessTables(unittest.TestCase):
    def test_deploy_simple_tables_right_tables(self):
        dynamodb = DynamoDB()

        with create_serverless_tables(SIMPLE_SERVERLESS_YML):
            # Tables are created
            tables = dynamodb.list_tables()
            self.assertCountEqual(tables, ["MyExampleTable", "AnotherTable"])

        # Tables are deleted
        self.assertCountEqual(dynamodb.list_tables(), [])

    def test_deploy_simple_tables_describe_tables(self):
        dynamodb = DynamoDB()

        with create_serverless_tables(SIMPLE_SERVERLESS_YML):
            table_desc_1 = dynamodb.client.describe_table(
                TableName="MyExampleTable",
            )
            table_desc_2 = dynamodb.client.describe_table(
                TableName="AnotherTable",
            )

        # Deployed with accurate descriptions
        self.assertEqual(len(table_desc_1["Table"]["KeySchema"]), 2)
        self.assertEqual(len(table_desc_2["Table"]["KeySchema"]), 1)

    def test_deploy_simple_tables_usable_by_awstin(self):
        dynamodb = DynamoDB()

        class ExampleModel(DynamoModel):
            _table_name_ = "MyExampleTable"

            myHashKey = Key()
            mySortKey = Key()
            another_attr = Attr()

        with create_serverless_tables(SIMPLE_SERVERLESS_YML):
            example_table = dynamodb[ExampleModel]
            item = ExampleModel(
                myHashKey="aaa",
                mySortKey=123,
                another_attr="Hello!",
            )
            example_table.put_item(item)

            retrieved_item = example_table[("aaa", 123)]

        self.assertEqual(retrieved_item.another_attr, "Hello!")
