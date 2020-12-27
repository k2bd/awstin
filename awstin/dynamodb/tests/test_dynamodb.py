import unittest
from contextlib import ExitStack

import awstin.dynamodb.table as ddb_table
from awstin.dynamodb import Attr, DynamoDB, DynamoModel, Key
from awstin.dynamodb.testing import temporary_dynamodb_table


class ModelWithoutSortkey(DynamoModel):
    _table_name_ = "test_without_sort"

    hashkey = Key()

    another_attr = Attr()

    def __eq__(self, other):
        if isinstance(other, ModelWithoutSortkey):
            return (
                self.hashkey == other.hashkey
                and self.another_attr == other.another_attr
            )
        return NotImplemented


class ModelWithSortkey(DynamoModel):
    _table_name_ = "test_with_sort"

    hashkey = Key()

    sortkey = Key()

    another_attr = Attr()

    def __eq__(self, other):
        if isinstance(other, ModelWithSortkey):
            return (
                self.hashkey == other.hashkey
                and self.sortkey == other.sortkey
                and self.another_attr == other.another_attr
            )
        return NotImplemented


class TestDynamoDB(unittest.TestCase):
    def setUp(self):
        self.table_without_sortkey = temporary_dynamodb_table(
            ModelWithoutSortkey,
            "hashkey",
        )

        self.table_with_sortkey = temporary_dynamodb_table(
            ModelWithSortkey,
            "hashkey",
            sortkey_name="sortkey",
            sortkey_type="N"
        )

    def test_dynamodb_table(self):
        with self.table_without_sortkey as table:
            test_item = ModelWithoutSortkey(
                hashkey="test_value",
                another_attr=5.5,
            )

            # Table can be used
            table.put_item(test_item)
            result_item = table["test_value"]
            self.assertIsInstance(result_item, ModelWithoutSortkey)
            self.assertEqual(result_item, test_item)

            # DynamoDB can access table
            dynamodb = DynamoDB()
            ddb_result = dynamodb[ModelWithoutSortkey]["test_value"]
            self.assertIsInstance(ddb_result, ModelWithoutSortkey)
            self.assertEqual(ddb_result, test_item)

    def test_dynamodb_table_access_by_shorthand_composite_key(self):
        with self.table_with_sortkey as table:
            test_item = ModelWithSortkey(
                hashkey="aaa",
                sortkey=555,
                another_attr="bb",
            )
            table.put_item(test_item)

            self.assertEqual(table[("aaa", 555)], test_item)

    def test_dynamodb_table_access_by_full_hash_key(self):
        with self.table_without_sortkey as table:
            test_item = ModelWithoutSortkey(
                hashkey="abcd",
                another_attr=777,
            )
            table.put_item(test_item)

            self.assertEqual(table[{"hashkey": "abcd"}], test_item)

    def test_dynamodb_table_access_by_full_composite_key(self):
        with self.table_with_sortkey as table:
            test_item = ModelWithSortkey(
                hashkey="aaa",
                sortkey=555,
                another_attr="bb",
            )
            table.put_item(test_item)

            self.assertEqual(
                table[{"hashkey": "aaa", "sortkey": 555}],
                test_item
            )

    def test_dynamodb_get_item_with_unset_attribute(self):
        self.fail("Todo")

    def test_dynamodb_get_item_attribute_not_in_model(self):
        self.fail("Todo")

    def test_dynamodb_table_delete_item(self):
        with self.table_without_sortkey as table:
            test_item = ModelWithoutSortkey(
                hashkey="123",
                another_attr="bb",
            )
            table.put_item(test_item)

            self.assertEqual(table["123"], test_item)

            table.delete_item("123")

            with self.assertRaises(KeyError):
                table["123"]

    def test_dynamodb_list_tables(self):
        with self.table_with_sortkey:
            with self.table_without_sortkey:
                tables = DynamoDB().list_tables()

        self.assertEqual(tables, ["test_with_sort", "test_without_sort"])

    def test_dynamodb_list_tables_long(self):
        # Reduce page size so test doesn't have to make over 100 tables
        ddb_table._PAGE_SIZE = 5

        dynamodb = DynamoDB()

        with ExitStack() as stack:
            for i in range(11):
                class Cls(DynamoModel):
                    _table_name_ = "abc"+str(i)
                table_ctx = temporary_dynamodb_table(Cls, "def"+str(i))
                stack.enter_context(table_ctx)

            self.assertEqual(len(dynamodb.list_tables()), 11)

    def test_dynamodb_table_items_generator(self):
        class BigItem(DynamoModel):
            _table_name_ = "test_tab"
            pkey = Key()
            bigstring = Attr()

            def __eq__(self, other):
                if isinstance(other, BigItem):
                    return (
                        self.pkey == other.pkey
                        and self.bigstring == other.bigstring
                    )
                return NotImplemented

        # Page size of DynamoDB item returns is 1MB. Add ~2.2MB of items
        add_items = [
            BigItem(
                pkey="item "+str(i),
                bigstring="a" * 100000
            )
            for i in range(22)
        ]
        with temporary_dynamodb_table(BigItem, "pkey") as table:
            for item in add_items:
                table.put_item(item)

            result_items = list(table.scan())

        self.assertEqual(len(result_items), 22)
        self.assertCountEqual(result_items, add_items)

    def test_dynamodb_table_scan_filter(self):
        self.fail("Todo")
