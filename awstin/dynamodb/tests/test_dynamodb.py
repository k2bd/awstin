from contextlib import ExitStack
import unittest

from awstin.dynamodb.api import DynamoDB
from awstin.dynamodb.testing import temporary_dynamodb_table


class TestDynamodb(unittest.TestCase):
    def test_dynamodb_table(self):
        with temporary_dynamodb_table("table_name", "hashkey_name") as table:
            test_item = {
                "hashkey_name": "test_value",
                "another_key": 5,
            }

            # Table can be used
            table.put_item(test_item)
            result_item = table["test_value"]
            self.assertEqual(result_item, test_item)

            # DynamoDB can access table
            dynamodb = DynamoDB()
            self.assertEqual(dynamodb["table_name"]["test_value"], test_item)

    def test_dynamodb_table_access_by_full_primary_key(self):
        with temporary_dynamodb_table("tabname", "hashname") as table:
            test_item = {"hashname": "123", "nothashname": "456"}
            table.put_item(test_item)

            self.assertEqual(table[{"hashname": "123"}], test_item)

    def test_dynamodb_table_delete_item(self):
        with temporary_dynamodb_table("tabname", "hashname") as table:
            test_item = {"hashname": "123", "nothashname": "456"}
            table.put_item(test_item)

            self.assertEqual(table["123"], test_item)

            table.delete_item("123")

            with self.assertRaises(KeyError):
                table["123"]

    def test_dynamodb_list_tables(self):
        with temporary_dynamodb_table("test_tab1", "test_key1"):
            with temporary_dynamodb_table("test_tab2", "test_key2"):
                tables = DynamoDB().tables

        self.assertEqual(tables, ["test_tab1", "test_tab2"])

    def test_dynamodb_list_tables_long(self):
        dynamodb = DynamoDB()

        table_managers = [
            temporary_dynamodb_table("abc"+str(i), "def"+str(i))
            for i in range(123)
        ]
        with ExitStack() as stack:
            for table in table_managers:
                stack.enter_context(table)

            self.assertEqual(len(dynamodb.tables), 123)
