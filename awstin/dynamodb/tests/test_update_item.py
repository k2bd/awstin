import unittest

from awstin.dynamodb.orm import Attr, DynamoModel, Key
from awstin.dynamodb.testing import temporary_dynamodb_table


class MyModel(DynamoModel):
    _table_name_ = "temp"

    pkey = Key()

    an_attr = Attr()

    another_attr = Attr()

    set_attr = Attr()

    third_attr = Attr()

    def __eq__(self, other):
        if isinstance(other, MyModel):
            return (
                (self.pkey == other.pkey)
                & (self.an_attr == other.an_attr)
                & (self.another_attr == other.another_attr)
                & (self.set_attr == other.set_attr)
                & (self.third_attr == other.third_attr)
            )
        return NotImplemented


class TestUpdateItem(unittest.TestCase):
    def setUp(self):
        self.temp_table = temporary_dynamodb_table(MyModel, "pkey")

    def test_update_set_value(self):
        update_expression = MyModel.an_attr.set(100)

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=100,
            )
            self.assertEqual(result, expected)

    def test_update_set_attr(self):
        update_expression = MyModel.an_attr.set(MyModel.another_attr)

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
                another_attr=22,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=22,
                another_attr=22,
            )
            self.assertEqual(result, expected)

    def test_update_set_attr_plus_value(self):
        update_expression = MyModel.an_attr.set(MyModel.an_attr + 100)

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=111,
            )
            self.assertEqual(result, expected)

    def test_update_set_attr_minus_value(self):
        update_expression = MyModel.an_attr.set(100 - MyModel.another_attr)

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
                another_attr=10,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=90,
                another_attr=10,
            )
            self.assertEqual(result, expected)

    def test_update_set_attr_plus_attr(self):
        update_expression = MyModel.an_attr.set(MyModel.an_attr + MyModel.another_attr)

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
                another_attr=22,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=33,
                another_attr=22,
            )
            self.assertEqual(result, expected)

    def test_update_multiple_sets(self):
        update_expression = MyModel.an_attr.set(
            MyModel.an_attr + MyModel.another_attr
        ) & MyModel.another_attr.set(50)

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
                another_attr=22,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=33,
                another_attr=50,
            )
            self.assertEqual(result, expected)

    def test_update_remove(self):
        update_expression = MyModel.an_attr.remove()

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
                another_attr=22,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                another_attr=22,
            )
            self.assertEqual(result, expected)

    def test_update_multiple_remove(self):
        update_expression = MyModel.an_attr.remove() & MyModel.another_attr.remove()

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
                another_attr=22,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
            )
            self.assertEqual(result, expected)

    def test_update_delete_int_set(self):
        update_expression = MyModel.an_attr.delete([2, 3, 4, 5])

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr={1, 3, 5, 7},
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr={1, 7},
            )
            self.assertEqual(result, expected)

    def test_update_delete_float_set(self):
        update_expression = MyModel.an_attr.delete([2.2, 3.3, 4.4, 5.5])

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr={1.1, 3.3, 5.5, 7.7},
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr={1.1, 7.7},
            )
            self.assertEqual(result, expected)

    def test_update_delete_string_set(self):
        update_expression = MyModel.an_attr.delete(["b", "c", "d", "e"])

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr={"a", "c", "e", "g"},
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr={"a", "g"},
            )
            self.assertEqual(result, expected)

    def test_update_delete_all(self):
        update_expression = MyModel.an_attr.delete(["a", "c", "e", "g"])

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr={"a", "c", "e", "g"},
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
            )
            self.assertEqual(result, expected)

    def test_update_delete_multiple(self):
        update_expression = MyModel.an_attr.delete(
            ["b", "c", "d", "e"]
        ) & MyModel.another_attr.delete([1, 2, 3])

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr={"a", "c", "e", "g"},
                another_attr={1, 2, 3},
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr={"a", "g"},
            )
            self.assertEqual(result, expected)

    def test_update_add_numerical(self):
        update_expression = MyModel.an_attr.add(50)

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=61,
            )
            self.assertEqual(result, expected)

    def test_update_add_set(self):
        update_expression = MyModel.an_attr.add([1, 2, 3])

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr={2, 3, 4},
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr={1, 2, 3, 4},
            )
            self.assertEqual(result, expected)

    def test_update_add_nonexistent_numerical(self):
        update_expression = MyModel.another_attr.add(50)

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=11,
                another_attr=50,
            )
            self.assertEqual(result, expected)

    def test_update_add_nonexistent_set(self):
        update_expression = MyModel.another_attr.add([1, 2, 3])

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=11,
                another_attr={1, 2, 3},
            )
            self.assertEqual(result, expected)

    def test_update_add_multiple(self):
        update_expression = MyModel.another_attr.add([1, 2, 3]) & MyModel.an_attr.add(
            20
        )

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=31,
                another_attr={1, 2, 3},
            )
            self.assertEqual(result, expected)

    def test_update_many(self):
        update_expression = (
            MyModel.an_attr.set(5 - MyModel.another_attr)
            & MyModel.third_attr.add(100)
            & MyModel.another_attr.remove()
            & MyModel.set_attr.delete([2, 3])
        )

        with self.temp_table as table:
            item = MyModel(
                pkey="aaa",
                an_attr=11,
                another_attr=22,
                set_attr={1, 3, 4, 5},
                third_attr=33,
            )

            table.put_item(item)

            result = table.update_item("aaa", update_expression)

            expected = MyModel(
                pkey="aaa",
                an_attr=5 - item.another_attr,
                third_attr=133,
                set_attr={1, 4, 5},
            )

            self.assertEqual(result, expected)

            got_item = table["aaa"]
            self.assertEqual(result, got_item)

    def test_update_with_false_conditions(self):
        update_expression = MyModel.an_attr.add(50) & MyModel.another_attr.remove()
        condition_expression = MyModel.an_attr > 11

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
                another_attr=100,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression, condition_expression)

            self.assertIsNone(result)

            # Item unchanged in the table
            self.assertEqual(table["bbb"], item)

    def test_update_with_conditions(self):
        update_expression = MyModel.an_attr.add(50) & MyModel.another_attr.remove()
        condition_expression = MyModel.an_attr == 11

        with self.temp_table as table:
            item = MyModel(
                pkey="bbb",
                an_attr=11,
                another_attr=100,
            )
            table.put_item(item)

            result = table.update_item("bbb", update_expression, condition_expression)

            expected = MyModel(
                pkey="bbb",
                an_attr=61,
            )
            self.assertEqual(result, expected)
