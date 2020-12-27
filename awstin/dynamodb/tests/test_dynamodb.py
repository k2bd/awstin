import unittest
from contextlib import ExitStack

import awstin.dynamodb.table as ddb_table
from awstin.dynamodb import NOT_SET, Attr, DynamoDB, DynamoModel, Key
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
            ModelWithSortkey, "hashkey", sortkey_name="sortkey", sortkey_type="N"
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

            self.assertEqual(table[{"hashkey": "aaa", "sortkey": 555}], test_item)

    def test_dynamodb_get_item_with_unset_attribute(self):
        with self.table_without_sortkey as table:
            test_item = ModelWithoutSortkey(
                hashkey="abcd",
            )
            self.assertTrue(test_item.another_attr is NOT_SET)
            table.put_item(test_item)

            result_item = table["abcd"]
            self.assertEqual(result_item, test_item)
            self.assertTrue(result_item.another_attr is NOT_SET)

    def test_dynamodb_get_item_attribute_not_in_model(self):
        with self.table_without_sortkey as table:
            table._boto3_table.put_item(
                Item={
                    "hashkey": "abcde",
                    "another_attr": "vvv",
                    "not_in_model": 555,
                    "also_not_in_model": "aaaa",
                }
            )

            expected = ModelWithoutSortkey(
                hashkey="abcde",
                another_attr="vvv",
            )

            self.assertEqual(table["abcde"], expected)

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
                    _table_name_ = "abc" + str(i)

                table_ctx = temporary_dynamodb_table(Cls, "def" + str(i))
                stack.enter_context(table_ctx)

            self.assertEqual(len(dynamodb.list_tables()), 11)

    def test_dynamodb_table_items_generator(self):
        class BigItem(DynamoModel):
            _table_name_ = "test_tab"
            pkey = Key()
            bigstring = Attr()

            def __eq__(self, other):
                if isinstance(other, BigItem):
                    return self.pkey == other.pkey and self.bigstring == other.bigstring
                return NotImplemented

        # Page size of DynamoDB item returns is 1MB. Add ~2.2MB of items
        add_items = [
            BigItem(pkey="item " + str(i), bigstring="a" * 100000) for i in range(22)
        ]
        with temporary_dynamodb_table(BigItem, "pkey") as table:
            for item in add_items:
                table.put_item(item)

            result_items = list(table.scan())

        self.assertEqual(len(result_items), 22)
        self.assertCountEqual(result_items, add_items)

    def test_dynamo_model_floats(self):
        temp_table = temporary_dynamodb_table(
            ModelWithoutSortkey,
            "hashkey",
            hashkey_type="N",
        )
        with temp_table as table:
            new_item = ModelWithoutSortkey(
                hashkey=9.99,
                another_attr=55.1,
            )
            table.put_item(new_item)

            retrieved_item = table[9.99]
            self.assertEqual(retrieved_item, new_item)
            self.assertIsInstance(retrieved_item.hashkey, float)
            self.assertIsInstance(retrieved_item.another_attr, float)

    def test_dynamo_model_list(self):
        with self.table_without_sortkey as table:
            new_item = ModelWithoutSortkey(
                hashkey="aaa",
                another_attr=[55.1, 9.22, 100.1, 50.5],
            )
            table.put_item(new_item)

            retrieved_item = table["aaa"]
            self.assertEqual(retrieved_item, new_item)
            self.assertIsInstance(retrieved_item.another_attr, list)
            for item in retrieved_item.another_attr:
                self.assertIsInstance(item, float)

    def test_dynamo_model_set(self):
        with self.table_without_sortkey as table:
            new_item = ModelWithoutSortkey(
                hashkey="aaa",
                another_attr=set([55.1, 9.22, 100.1, 50.6]),
            )
            table.put_item(new_item)

            retrieved_item = table["aaa"]
            self.assertEqual(retrieved_item, new_item)
            self.assertIsInstance(retrieved_item.another_attr, set)
            for item in retrieved_item.another_attr:
                self.assertIsInstance(item, float)

    def test_dynamo_model_tuple(self):
        with self.table_without_sortkey as table:
            new_item = ModelWithoutSortkey(
                hashkey="aaa",
                another_attr=(55.1, 9.22, 100.1, 50.5),
            )
            table.put_item(new_item)

            retrieved_item = table["aaa"]

            expected = ModelWithoutSortkey(
                hashkey="aaa",
                another_attr=[55.1, 9.22, 100.1, 50.5],
            )
            # Tuples converted to lists
            self.assertNotEqual(retrieved_item, new_item)
            self.assertEqual(retrieved_item, expected)
            self.assertIsInstance(retrieved_item.another_attr, list)
            for item in retrieved_item.another_attr:
                self.assertIsInstance(item, float)

    def test_dynamo_model_dict(self):
        with self.table_without_sortkey as table:
            new_item = ModelWithoutSortkey(
                hashkey="aaa",
                another_attr={"hello": 2.2, "world": 4.4},
            )
            table.put_item(new_item)

            retrieved_item = table["aaa"]
            self.assertEqual(retrieved_item, new_item)
            self.assertIsInstance(retrieved_item.another_attr, dict)
            for value in retrieved_item.another_attr.values():
                self.assertIsInstance(value, float)

    def test_dynamo_model_different_attr_name(self):
        class Model(DynamoModel):
            _table_name_ = "diff_attr_table"
            hash_key = Key("actual_key_name")
            an_attr = Attr("actual_attr_name")

            def __eq__(self, other):
                if isinstance(other, Model):
                    return (
                        self.hash_key == other.hash_key
                        and self.an_attr == other.an_attr
                    )
                return NotImplemented

        with temporary_dynamodb_table(Model, "actual_key_name") as table:
            test_item = Model(
                hash_key="a hash key",
                an_attr=12345,
            )
            table.put_item(test_item)

            # Item is serialized with the defined attribute names
            boto_item = table._boto3_table.get_item(
                Key={"actual_key_name": "a hash key"},
            )["Item"]
            self.assertEqual(
                boto_item,
                {
                    "actual_key_name": "a hash key",
                    "actual_attr_name": 12345,
                },
            )

            self.assertEqual(table["a hash key"], test_item)

    def test_filter_begins_with(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="abchello",
                another_attr="defworld",
            )
            miss_key = ModelWithoutSortkey(
                hashkey="hello",
                another_attr="defworld",
            )
            miss_attr = ModelWithoutSortkey(
                hashkey="abchello2",
                another_attr="world",
            )

            table.put_item(hit)
            table.put_item(miss_key)
            table.put_item(miss_attr)

            scan_filter = ModelWithoutSortkey.hashkey.begins_with(
                "abc"
            ) & ModelWithoutSortkey.another_attr.begins_with("def")

            items = list(table.scan(scan_filter=scan_filter))

            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_between(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="carving",
                another_attr=1.1,
            )
            miss_key = ModelWithoutSortkey(
                hashkey="yellow",
                another_attr=2.2,
            )
            miss_attr = ModelWithoutSortkey(
                hashkey="dartboard",
                another_attr=9.5,
            )

            table.put_item(hit)
            table.put_item(miss_key)
            table.put_item(miss_attr)

            scan_filter = ModelWithoutSortkey.hashkey.between(
                "a", "e"
            ) & ModelWithoutSortkey.another_attr.between(0.5, 4.12)

            items = list(table.scan(scan_filter=scan_filter))

            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_eq(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="interesting",
                another_attr=55,
            )
            miss = ModelWithoutSortkey(
                hashkey="typewriter",
                another_attr=100,
            )

            table.put_item(hit)
            table.put_item(miss)

            key_filter = ModelWithoutSortkey.hashkey == "interesting"

            items = list(table.scan(scan_filter=key_filter))
            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

            attr_filter = ModelWithoutSortkey.another_attr == 55

            items = list(table.scan(scan_filter=attr_filter))
            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_gt(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="hello",
                another_attr=77.5,
            )
            miss_key = ModelWithoutSortkey(
                hashkey="afternoon",
                another_attr=20.1,
            )
            miss_attr = ModelWithoutSortkey(
                hashkey="kangaroo",
                another_attr=15,
            )

            table.put_item(hit)
            table.put_item(miss_key)
            table.put_item(miss_attr)

            scan_filter = (ModelWithoutSortkey.hashkey > "d") & (
                ModelWithoutSortkey.another_attr > 20
            )

            items = list(table.scan(scan_filter=scan_filter))

            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_ge(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="hello",
                another_attr=20.1,
            )
            miss_key = ModelWithoutSortkey(
                hashkey="afternoon",
                another_attr=30,
            )
            miss_attr = ModelWithoutSortkey(
                hashkey="kangaroo",
                another_attr=15,
            )

            table.put_item(hit)
            table.put_item(miss_key)
            table.put_item(miss_attr)

            scan_filter = (ModelWithoutSortkey.hashkey >= "h") & (
                ModelWithoutSortkey.another_attr >= 20.1
            )

            items = list(table.scan(scan_filter=scan_filter))

            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_lt(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="aardvark",
                another_attr=1,
            )
            miss_key = ModelWithoutSortkey(
                hashkey="zebra",
                another_attr=2,
            )
            miss_attr = ModelWithoutSortkey(
                hashkey="anteater",
                another_attr=3,
            )

            table.put_item(hit)
            table.put_item(miss_key)
            table.put_item(miss_attr)

            scan_filter = (ModelWithoutSortkey.hashkey < "c") & (
                ModelWithoutSortkey.another_attr < 2.5
            )

            items = list(table.scan(scan_filter=scan_filter))

            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_le(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="azzzzzzz",
                another_attr=1,
            )
            miss_key = ModelWithoutSortkey(
                hashkey="zebra",
                another_attr=2,
            )
            miss_attr = ModelWithoutSortkey(
                hashkey="anteater",
                another_attr=3,
            )

            table.put_item(hit)
            table.put_item(miss_key)
            table.put_item(miss_attr)

            scan_filter = (ModelWithoutSortkey.hashkey <= "azzzzzzz") & (
                ModelWithoutSortkey.another_attr <= 2
            )

            items = list(table.scan(scan_filter=scan_filter))

            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_attribute_type(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="interesting",
                another_attr=55,
            )
            miss = ModelWithoutSortkey(
                hashkey="typewriter",
                another_attr="hello",
            )

            table.put_item(hit)
            table.put_item(miss)

            attr_filter = ModelWithoutSortkey.another_attr.attribute_type("N")

            items = list(table.scan(scan_filter=attr_filter))
            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_contains(self):
        with self.table_without_sortkey as table:
            hit1 = ModelWithoutSortkey(
                hashkey="interesting",
                another_attr=[1.1, 2.2, 3.3, 4.4, 5.5],
            )
            hit2 = ModelWithoutSortkey(
                hashkey="feathered",
                another_attr=[2.2],
            )
            miss1 = ModelWithoutSortkey(
                hashkey="typewriter",
                another_attr=2.2,
            )
            miss2 = ModelWithoutSortkey(
                hashkey="ghost",
                another_attr=[1.1, 2, 3, 99],
            )

            table.put_item(hit1)
            table.put_item(hit2)
            table.put_item(miss1)
            table.put_item(miss2)

            attr_filter = ModelWithoutSortkey.another_attr.contains(2.2)

            items = list(table.scan(scan_filter=attr_filter))
            self.assertEqual(len(items), 2)
            self.assertCountEqual(items, [hit1, hit2])

    def test_filter_exists(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="interesting",
                another_attr=55,
            )
            miss = ModelWithoutSortkey(
                hashkey="typewriter",
            )

            table.put_item(hit)
            table.put_item(miss)

            attr_filter = ModelWithoutSortkey.another_attr.exists()

            items = list(table.scan(scan_filter=attr_filter))
            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_in(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="interesting",
                another_attr=55,
            )
            miss = ModelWithoutSortkey(
                hashkey="typewriter",
                another_attr=99,
            )

            table.put_item(hit)
            table.put_item(miss)

            attr_filter = ModelWithoutSortkey.another_attr.in_([55, 100])

            items = list(table.scan(scan_filter=attr_filter))
            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_ne(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="interesting",
                another_attr=55,
            )
            miss = ModelWithoutSortkey(
                hashkey="typewriter",
                another_attr=99,
            )

            table.put_item(hit)
            table.put_item(miss)

            attr_filter = ModelWithoutSortkey.another_attr != 99

            items = list(table.scan(scan_filter=attr_filter))
            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

    def test_filter_not_exists(self):
        with self.table_without_sortkey as table:
            hit = ModelWithoutSortkey(
                hashkey="interesting",
            )
            miss = ModelWithoutSortkey(
                hashkey="typewriter",
                another_attr=55,
            )

            table.put_item(hit)
            table.put_item(miss)

            attr_filter = ModelWithoutSortkey.another_attr.not_exists()

            items = list(table.scan(scan_filter=attr_filter))
            self.assertEqual(len(items), 1)
            (item,) = items
            self.assertEqual(item, hit)

            self.assertTrue(item.another_attr is NOT_SET)
