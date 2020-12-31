import unittest

from awstin.dynamodb.orm import UpdateOperand, CombineOperand, Attr, CombineOperator, SetOperator, AddOperator, DeleteOperator, RemoveOperator


#TODO from print to assert
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
        operator = UpdateOperator(
            UpdateOperand(Attr(attribute_name="abcd")),
            "SET"
        )

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
            )
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
                )
            )
        )

        print(operator.serialize())
