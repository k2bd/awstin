import unittest
from contextlib import ExitStack

import awstin.dynamodb.api as ddb_api
from awstin.dynamodb import DynamoDB
from awstin.dynamodb.testing import temporary_dynamodb_table


class TestDynamoDB(unittest.TestCase):
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

    def test_dynamodb_table_access_by_shorthand_composite_key(self):
        temp_table = temporary_dynamodb_table(
            "tabname",
            "hashname",
            sortkey_name="sortname",
            sortkey_type="N",
        )
        with temp_table as table:
            test_item = {"hashname": "aaa", "sortname": 555, "aa": "bb"}
            table.put_item(test_item)

            self.assertEqual(table[("aaa", 555)], test_item)

    def test_dynamodb_table_access_by_full_hash_key(self):
        with temporary_dynamodb_table("tabname", "hashname") as table:
            test_item = {"hashname": "123", "nothashname": "456"}
            table.put_item(test_item)

            self.assertEqual(table[{"hashname": "123"}], test_item)

    def test_dynamodb_table_access_by_full_composite_key(self):
        temp_table = temporary_dynamodb_table(
            "tabname",
            "hashname",
            sortkey_name="sortname",
            sortkey_type="N",
        )
        with temp_table as table:
            test_item = {"hashname": "aaa", "sortname": 555, "aa": "bb"}
            table.put_item(test_item)

            self.assertEqual(
                table[{"hashname": "aaa", "sortname": 555}],
                test_item
            )

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
                tables = DynamoDB().list_tables()

        self.assertEqual(tables, ["test_tab1", "test_tab2"])

    def test_dynamodb_list_tables_long(self):
        # Reduce page size so test doesn't have to make over 100 tables
        ddb_api._PAGE_SIZE = 5

        dynamodb = DynamoDB()

        table_managers = [
            temporary_dynamodb_table("abc"+str(i), "def"+str(i))
            for i in range(11)
        ]
        with ExitStack() as stack:
            for table in table_managers:
                stack.enter_context(table)

            self.assertEqual(len(dynamodb.list_tables()), 11)

    def test_dynamodb_table_items_generator(self):
        # Page size of DynamoDB item returns is 1MB. Add ~2.2MB of items
        add_items = [
            {"pkey": "item "+str(i), "bigstring": "a" * 100000}
            for i in range(22)
        ]
        with temporary_dynamodb_table("test_tab", "pkey") as table:
            for item in add_items:
                table.put_item(item)

            result_items = list(table.scan())

        self.assertEqual(len(result_items), 22)
        self.assertCountEqual(result_items, add_items)
