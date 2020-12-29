from abc import abstractmethod, ABC
from collections import defaultdict
import uuid
from typing import Union

from boto3.dynamodb.conditions import Attr as BotoAttr
from boto3.dynamodb.conditions import Key as BotoKey

from awstin.dynamodb.utils import from_decimal, to_decimal


class NotSet:
    """
    A value of an attribute on a data model is not present in a DynamoDB result
    """

    def __str__(self):
        return "<<Attribute not set>>"

    def __repr__(self):
        return "<<Attribute not set>>"


NOT_SET = NotSet()


class BaseAttribute:
    def __init__(self, attribute_name: Union[str, None] = None):
        # Set by user
        self._attribute_name = attribute_name

        # Set by Model
        self._name_on_model = None

    @property
    def name(self):
        if self._attribute_name is not None:
            return self._attribute_name
        else:
            return self._name_on_model

    def __getattr__(self, name):
        """
        Support for nested mapping queries
        """
        try:
            return super().__getattr__(name)
        except AttributeError:
            return type(self)(attribute_name=f"{self.name}.{name}")

    def __getitem__(self, index):
        """
        Support for nested container queries
        """
        return type(self)(attribute_name=f"{self.name}[{index}]")

    # --- Query and scan filter expressions ---

    def begins_with(self, value):
        """
        Filter results by a key or attribute beginning with a value

        Parameters
        ----------
        value : str
            Starting string for returned results
        """
        return self._query_type(self.name).begins_with(to_decimal(value))

    def between(self, low, high):
        """
        Filter results by range (inclusive)

        Parameters
        ----------
        low : Any
            Low end of the range
        high : Any
            High end of the range
        """
        return self._query_type(self.name).between(
            to_decimal(low),
            to_decimal(high),
        )

    def __eq__(self, value):
        return self._query_type(self.name).eq(to_decimal(value))

    def __gt__(self, value):
        return self._query_type(self.name).gt(to_decimal(value))

    def __ge__(self, value):
        return self._query_type(self.name).gte(to_decimal(value))

    def __lt__(self, value):
        return self._query_type(self.name).lt(to_decimal(value))

    def __le__(self, value):
        return self._query_type(self.name).lte(to_decimal(value))

    def attribute_type(self, value):
        """
        Filter results by attribute type

        Parameters
        ----------
        value : str
            Index for a DynamoDB attribute type (e.g. "N" for Number)
        """
        return BotoAttr(self.name).attribute_type(to_decimal(value))

    def contains(self, value):
        """
        Filter results by attributes that are containers and contain the target
        value

        Parameters
        ----------
        values : Any
            Result must contain this item
        """
        return BotoAttr(self.name).contains(to_decimal(value))

    def exists(self):
        """
        Filter results by existence of an attribute
        """
        return BotoAttr(self.name).exists()

    def in_(self, values):
        """
        Filter results by existence in a set

        Parameters
        ----------
        values : list of Any
            Allowed values of returned results
        """
        in_values = [to_decimal(value) for value in values]
        return BotoAttr(self.name).is_in(in_values)

    def __ne__(self, value):
        return BotoAttr(self.name).ne(to_decimal(value))

    def not_exists(self):
        """
        Filter results by non-existence of an attribute
        """
        return BotoAttr(self.name).not_exists()

    # --- Update expressions ---

    def set(self, expression):
        pass

    def remove(self, expression):
        pass

    def add(self, expression):
        pass

    def delete(self):
        pass

    def __add__(self, other):
        pass

    def __sub__(self, other):
        pass


class Key(BaseAttribute):
    """
    Used to define and query hash and sort key attributes on a dynamodb table
    data model
    """

    _query_type = BotoKey


class Attr(BaseAttribute):
    """
    Used to define and query non-key attributes on a dynamodb table data model
    """

    _query_type = BotoAttr


class DynamoModelMeta(type):
    def __getattribute__(self, name):
        attr = super().__getattribute__(name)
        if isinstance(attr, BaseAttribute):
            attr._name_on_model = name
            return attr
        else:
            return attr

    def _dynamodb_attributes(self):
        result = {
            getattr(self, attr).name: attr
            for attr in dir(self)
            if isinstance(getattr(self, attr), BaseAttribute)
        }
        return result

    def _get_kwargs(self):
        """
        Kwargs that should be passed to query, scan, get_item
        """
        return {
            **self._dynamo_projection(),
            **self._index_kwargs(),
        }

    def _dynamo_projection(self):
        """
        Attributes to request when retrieving data from DynamoDB

        Returns
        -------
        dict
            kwargs to be passed to DynamoDB get attribute calls to employ
            a projection expression and placeholders
        """
        placeholders = {
            "#" + str(uuid.uuid4())[:8]: value
            for value in self._dynamodb_attributes().keys()
        }
        expression = ", ".join(placeholders.keys())
        return dict(
            ProjectionExpression=expression,
            ExpressionAttributeNames=placeholders,
        )

    def _index_kwargs(self):
        if hasattr(self, "_index_name_"):
            return dict(
                IndexName=self._index_name_,
            )
        else:
            return {}


class DynamoModel(metaclass=DynamoModelMeta):
    """
    Abstract class for defining DynamoDB data models.

    For example:
    ```python
    class MyDataModel(DynamoModel):
        _table_name_ = "MyTable"

        hashkey_name = HashKey()

        # The name of the DynamoDB attribute differs from the
        # name on the data model
        sortkey = SortKey("sortkeyName")

        an_attribute = Attr()

        another_attribute = Attr("attributeName")
    ```
    """

    def __init__(self, **kwargs):
        model_attrs = type(self)._dynamodb_attributes().values()

        for name in model_attrs:
            setattr(self, name, NOT_SET)

        for name, value in kwargs.items():
            if name not in model_attrs:
                msg = f"{type(self)!r} has no attribute {name!r}"
                raise AttributeError(msg)
            setattr(self, name, value)

    @classmethod
    def _from_dynamodb(cls, data):
        model_attrs = cls._dynamodb_attributes()

        result = cls()

        for attr in model_attrs.values():
            setattr(result, attr, NOT_SET)

        for db_attr, value in data.items():
            if db_attr in model_attrs.keys():
                if type(value) in [list, set, tuple]:
                    value = type(value)(from_decimal(v) for v in value)
                elif type(value) is dict:
                    value = {from_decimal(k): from_decimal(v) for k, v in value.items()}
                else:
                    value = from_decimal(value)
                setattr(result, model_attrs[db_attr], value)

        return result

    def _to_dynamodb(self):
        model_attrs = type(self)._dynamodb_attributes()

        result = {}

        for dynamo_name, model_name in model_attrs.items():
            value = getattr(self, model_name)
            if value is not NOT_SET:
                if type(value) in [list, set, tuple]:
                    value = type(value)(to_decimal(v) for v in value)
                elif type(value) is dict:
                    value = {to_decimal(k): to_decimal(v) for k, v in value.items()}
                else:
                    value = to_decimal(value)
                result[dynamo_name] = value

        return result


# ---- Update Operators

class UpdateOperator(ABC):
    """
    TODO
    """
    def __and__(self, other):
        """
        Combine two update expressions
        """
        return Combine(self, other)

    @abstractmethod
    def update_dict(self):
        """
        Mapping of update expression section to list of values
        """
        pass

    def update_expression(self):
        pass

    def serialize(self):
        """
        Produce kwargs to be passed to DynamoDB Table.update_item.

        Keys and values are:
            "UpdateExpression": string representing the update expression
            "ExpressionAttributeNames": Placeholder map for attribute names
            "ExpressionAttributeValues": Placeholder map for attribute values

        Returns
        -------
        dict
            Kwargs for update_item
        """
        return {
            "UpdateExpression": self.update_expression(),
            "ExpressionAttributeNames": self.expression_attribute_names(),
            "ExpressionAttributeValues": self.expression_attribute_values(),
        }


class Combine(UpdateOperator):
    """
    Combine two update expressions
    """
    def __init__(self, left, right):
        super().__init__()
        self.left = left
        self.right = right

    def update_dict(self):
        result = defaultdict(list)
        items = (
            list(self.left.update_dict().items())
            + list(self.right.update_dict().items())
        )
        for key, values in items:
            result[key].extend(values)

        return result


class Set(UpdateOperator):
    """
    Update expression for SET
    """
    def __init__(self, operand):
        self.operand = operand

    def update_dict(self):
        return {
            "SET": [self.operand.serialize()]
        }


class Remove(UpdateOperator):
    """
    Update expression for REMOVE
    """
    def __init__(self, operand):
        self.operand = operand

    def update_dict(self):
        return {
            "REMOVE": [self.operand.serialize()]
        }


class Add(UpdateOperator):
    """
    Update expression for ADD
    """
    def __init__(self, operand):
        self.operand = operand

    def update_dict(self):
        return {
            "ADD": [self.operand.serialize()]
        }


class Delete(UpdateOperator):
    """
    Update expression for DELETE
    """
    def __init__(self, operand):
        self.operand = operand

    def update_dict(self):
        return {
            "DELETE": [self.operand.serialize()]
        }


# ---- Update Operands

class UpdateOperand:
    """
    Inner part of an update expression
    """
    def __init__(self, value):
        self.value = value

    def serialize(self):
        name = str(uuid.uuid4())[:8]
        mapping = {name: str(self.value)}

        if isinstance(self.value, UpdateOperand):
            return self.value.serialize()
        elif isinstance(self.value, BaseAttribute):
            return {
                "UpdateExpression": name,
                "ExpressionAttributeNames": mapping,
                "ExpressionAttributeValues": {},
            }
        else:
            return {
                "UpdateExpression": name,
                "ExpressionAttributeNames": {},
                "ExpressionAttributeValues": mapping,
            }


class Plus(UpdateOperand):
    """
    Add two expressions
    """
    def __init__(self, left, right):
        self.left = left
        self.right = right


class Minus(UpdateOperand):
    def __init__(self, left, right):
        self.left = left
        self.right = right

