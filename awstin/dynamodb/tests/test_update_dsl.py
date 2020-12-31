import unittest

from awstin.dynamodb.orm import (
    UpdateOperand,
    CombineOperand,
    Attr,
    CombineOperator,
    SetOperator,
    AddOperator,
    DeleteOperator,
    RemoveOperator,
    DynamoModel,
    Key,
)
from awstin.dynamodb.testing import temporary_dynamodb_table


class MyModel(DynamoModel):
    _table_name_ = "temp"

    pkey = Key()

    an_attr = Attr()

    another_attr = Attr()

    third_attr = Attr()


# TODO from print to assert
class TestUpdateDSL(unittest.TestCase):
    def test_combine_operand_value_value(self):
        operand = CombineOperand(1, 2, "+")

        print(operand.serialize())

    def test_combine_operand_operand_value(self):
        operand = CombineOperand(
            UpdateOperand(Attr(attribute_name="abcd")),
            2,
            "+",
        )

        print(operand.serialize())

    def test_combine_operand_operand_combineoperand(self):
        operand = CombineOperand(
            UpdateOperand(Attr(attribute_name="abcd")),
            CombineOperand(
                4,
                UpdateOperand(
                    Attr(attribute_name="bbb"),
                ),
                "-",
            ),
            "+",
        )

        print(operand.serialize())

    def test_temp1(self):
        operator = UpdateOperator(UpdateOperand(Attr(attribute_name="abcd")), "SET")

        print(operator.serialize())

    def test_temp2(self):
        operator = CombineOperator(
            DeleteOperator(
                UpdateOperand(Attr(attribute_name="abcd")),
            ),
            SetOperator(
                Attr(attribute_name="efgh"),
                CombineOperand(
                    Attr(attribute_name="efgh"),
                    4,
                    "+",
                ),
            ),
        )

        print(operator.serialize())

    def test_temp3(self):
        operator = CombineOperator(
            SetOperator(
                Attr(attribute_name="bbbb"),
                UpdateOperand(Attr(attribute_name="cccc")),
            ),
            CombineOperator(
                DeleteOperator(
                    UpdateOperand(Attr(attribute_name="abcd")),
                ),
                SetOperator(
                    Attr(attribute_name="efgh"),
                    CombineOperand(
                        Attr(attribute_name="efgh"),
                        4,
                        "+",
                    ),
                ),
            ),
        )

        print(operator.serialize())

    def test_temp4(self):
        update_expression = (
            MyModel.an_attr.set(MyModel.another_attr + 5)
            & MyModel.third_attr.add(100)
            & MyModel.another_attr.remove()
        )

        print(update_expression.serialize())

    def test_temp5(self):
        update_expression = (
            MyModel.an_attr.set(MyModel.another_attr + 5)
            & MyModel.third_attr.add(100)
            & MyModel.another_attr.remove()
        )

        with temporary_dynamodb_table(MyModel, "pkey") as table:
            item = MyModel(
                pkey="aaa",
                an_attr=11,
                another_attr=22,
                third_attr=33,
            )

            table.put_item(item)

            table.update_item("aaa", update_expression)

            print(table["aaa"]._to_dynamodb())
