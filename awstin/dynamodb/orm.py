import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
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
        """
        Parameters
        ----------
        attribute_name : str, optional
            Name of the property in the DynamoDB table. Defaults to the name of
            the attribute on the DynamoModel class.
        """
        # Set by user
        self._attribute_name = attribute_name

        # Set by Model
        self._name_on_model = None

    @property
    def _awstin_name(self):
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
            return type(self)(attribute_name=f"{self._awstin_name}.{name}")

    def __getitem__(self, index):
        """
        Support for nested container queries
        """
        return type(self)(attribute_name=f"{self._awstin_name}[{index}]")

    # --- Query and scan filter expressions ---

    def begins_with(self, value):
        """
        Filter results by a key or attribute beginning with a value

        Parameters
        ----------
        value : str
            Starting string for returned results
        """
        return self._query_type(self._awstin_name).begins_with(to_decimal(value))

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
        return self._query_type(self._awstin_name).between(
            to_decimal(low),
            to_decimal(high),
        )

    def __eq__(self, value):
        return self._query_type(self._awstin_name).eq(to_decimal(value))

    def __gt__(self, value):
        return self._query_type(self._awstin_name).gt(to_decimal(value))

    def __ge__(self, value):
        return self._query_type(self._awstin_name).gte(to_decimal(value))

    def __lt__(self, value):
        return self._query_type(self._awstin_name).lt(to_decimal(value))

    def __le__(self, value):
        return self._query_type(self._awstin_name).lte(to_decimal(value))

    def attribute_type(self, value):
        """
        Filter results by attribute type

        Parameters
        ----------
        value : str
            Index for a DynamoDB attribute type (e.g. "N" for Number)
        """
        return BotoAttr(self._awstin_name).attribute_type(to_decimal(value))

    def contains(self, value):
        """
        Filter results by attributes that are containers and contain the target
        value

        Parameters
        ----------
        values : Any
            Result must contain this item
        """
        return BotoAttr(self._awstin_name).contains(to_decimal(value))

    def exists(self):
        """
        Filter results by existence of an attribute
        """
        return BotoAttr(self._awstin_name).exists()

    def in_(self, values):
        """
        Filter results by existence in a set

        Parameters
        ----------
        values : list of Any
            Allowed values of returned results
        """
        in_values = [to_decimal(value) for value in values]
        return BotoAttr(self._awstin_name).is_in(in_values)

    def __ne__(self, value):
        return BotoAttr(self._awstin_name).ne(to_decimal(value))

    def not_exists(self):
        """
        Filter results by non-existence of an attribute
        """
        return BotoAttr(self._awstin_name).not_exists()

    def size(self):
        """
        Filter by size of a collection
        """
        return Size(self._awstin_name)

    # --- Update expressions ---

    def set(self, expression):
        """
        Set an attribute to a new value.
        Corresponds to SET as part of the update expression in
        ``Table.update_item``.

        Parameters
        ----------
        expression : UpdateOperand
            New value, or an expression defining a new value
        """
        return SetOperator(self, UpdateOperand(expression))

    def remove(self):
        """
        Remove an attribute.
        Corresponds to REMOVE as part of the update expression in
        ``Table.update_item``.
        """
        return RemoveOperator(self)

    def add(self, expression):
        """
        Add to an attribute (numerical add or addition to a set).
        Corresponds to ADD as part of the update expression in
        ``Table.update_item``.

        Parameters
        ----------
        expression : UpdateOperand
            Value to add
        """
        return AddOperator(self, UpdateOperand(expression))

    def delete(self, expression):
        """
        Delete part of a set attribute.
        Corresponds to DELETE as part of the update expression in
        ``Table.update_item``.

        Parameters
        ----------
        expression : UpdateOperand
            Value to delete
        """
        return DeleteOperator(self, UpdateOperand(expression))

    def __add__(self, other):
        return CombineOperand(UpdateOperand(self), UpdateOperand(other), "+")

    def __sub__(self, other):
        return CombineOperand(UpdateOperand(self), UpdateOperand(other), "-")

    def __radd__(self, other):
        return CombineOperand(UpdateOperand(other), UpdateOperand(self), "+")

    def __rsub__(self, other):
        return CombineOperand(UpdateOperand(other), UpdateOperand(self), "-")

    def if_not_exists(self, value):
        """
        Conditionally return a value if this attribute doesn't exist on the
        model
        """
        return IfNotExistsOperand(UpdateOperand(self), UpdateOperand(value))


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


def size_query(self, *args, **kwargs):
    return BotoAttr(self._awstin_name).size()


class Size(BaseAttribute):
    _query_type = size_query


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
            getattr(self, attr)._awstin_name: attr
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
    Class defining an ORM model for a DynamoDB table.

    Subclasses must have a ``_table_name_`` attribute. Attributes making up
    the data model should be Attr or Key instances.

    Subclasses representing indexes should also have an ``_index_name_``
    attribute
    """

    def __init__(self, **kwargs):
        """
        Parameters
        ----------
        **kwargs : dict of (str, Any)
            Initialization of Attr and Key attributes.
        """
        model_attrs = type(self)._dynamodb_attributes().values()

        for name in model_attrs:
            setattr(self, name, NOT_SET)

        for name, value in kwargs.items():
            if name not in model_attrs:
                msg = f"{type(self)!r} has no attribute {name!r}"
                raise AttributeError(msg)
            setattr(self, name, value)

    @classmethod
    def deserialize(cls, data):
        """
        Deserialize JSON into a DynamoModel subclass. Internally converts
        Decimal to float in the deserialization.

        Parameters
        ----------
        data : dict of (str, Any)
            Serialized model

        Returns
        -------
        DynamoModel
            The deserialized data model
        """
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

    def serialize(self):
        """
        Serialize a DynamoModel subclass to JSON that can be inserted into
        DynamoDB. Internally converts float to Decimal.

        Returns
        -------
        dict of (str, Any)
            The serialized JSON entry
        """
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
    A representation of an UpdateItem expression
    """

    def __and__(self, other):
        """
        Combine two update expressions
        """
        return CombineOperator(self, other)

    @abstractmethod
    def update_dict(self):
        pass

    @staticmethod
    def update_expression(update_dict):
        expressions = []

        for operation in "SET", "ADD", "DELETE", "REMOVE":
            if update_dict.get(operation):
                expressions.append(operation + " " + ", ".join(update_dict[operation]))

        return " ".join(expressions)

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
        update_dict = self.update_dict()
        result = {
            "UpdateExpression": self.update_expression(update_dict),
        }
        if update_dict["ExpressionAttributeNames"]:
            result["ExpressionAttributeNames"] = update_dict["ExpressionAttributeNames"]
        if update_dict["ExpressionAttributeValues"]:
            result["ExpressionAttributeValues"] = update_dict[
                "ExpressionAttributeValues"
            ]
        return result


class CombineOperator(UpdateOperator):
    """
    Combine two update expressions
    """

    def __init__(self, left, right):
        self.left = left
        self.right = right

    def update_dict(self):
        result = defaultdict(list)
        ser_left = self.left.update_dict()
        ser_right = self.right.update_dict()

        items = list(ser_left.items()) + list(ser_right.items())
        for key, values in items:
            if key in ["SET", "ADD", "DELETE", "REMOVE"]:
                result[key].extend(values)

        result["ExpressionAttributeNames"] = dict(
            **ser_left["ExpressionAttributeNames"],
            **ser_right["ExpressionAttributeNames"],
        )
        result["ExpressionAttributeValues"] = dict(
            **ser_left["ExpressionAttributeValues"],
            **ser_right["ExpressionAttributeValues"],
        )

        return result


class SetOperator(UpdateOperator):
    """
    Support for SET
    """

    def __init__(self, attr, operand):
        self.attr = attr
        self.operand = operand

    def update_dict(self):
        serialized_attr = itemize_attr(self.attr)
        serialized_operand = self.operand.serialize()

        attribute_names = dict(
            **serialized_operand["ExpressionAttributeNames"],
            **serialized_attr["ExpressionAttributeNames"],
        )
        return {
            "SET": [
                f"{serialized_attr['UpdateExpression']} = "
                + serialized_operand["UpdateExpression"]
            ],
            "ExpressionAttributeNames": attribute_names,
            "ExpressionAttributeValues": serialized_operand[
                "ExpressionAttributeValues"
            ],
        }


class AddOperator(UpdateOperator):
    def __init__(self, attr, operand):
        self.attr = attr
        self.operand = operand

    def update_dict(self):
        serialized_attr = itemize_attr(self.attr)
        serialized_operand = self.operand.serialize()

        attribute_names = dict(
            **serialized_operand["ExpressionAttributeNames"],
            **serialized_attr["ExpressionAttributeNames"],
        )
        return {
            "ADD": [
                f"{serialized_attr['UpdateExpression']} "
                + serialized_operand["UpdateExpression"]
            ],
            "ExpressionAttributeNames": attribute_names,
            "ExpressionAttributeValues": serialized_operand[
                "ExpressionAttributeValues"
            ],
        }


class RemoveOperator(UpdateOperator):
    def __init__(self, attr):
        self.attr = attr

    def update_dict(self):
        serialized_attr = itemize_attr(self.attr)

        return {
            "REMOVE": [serialized_attr["UpdateExpression"]],
            "ExpressionAttributeNames": serialized_attr["ExpressionAttributeNames"],
            "ExpressionAttributeValues": {},
        }


class DeleteOperator(UpdateOperator):
    def __init__(self, attr, operand):
        self.attr = attr
        self.operand = operand

    def update_dict(self):
        serialized_attr = itemize_attr(self.attr)
        serialized_operand = self.operand.serialize()

        attribute_names = dict(
            **serialized_operand["ExpressionAttributeNames"],
            **serialized_attr["ExpressionAttributeNames"],
        )
        return {
            "DELETE": [
                f"{serialized_attr['UpdateExpression']} "
                + serialized_operand["UpdateExpression"]
            ],
            "ExpressionAttributeNames": attribute_names,
            "ExpressionAttributeValues": serialized_operand[
                "ExpressionAttributeValues"
            ],
        }


# ---- Update Operands


def serialize_operand(value):
    name = str(uuid.uuid4())[:8]

    if isinstance(value, UpdateOperand):
        return value.serialize()
    elif isinstance(value, BaseAttribute):
        return itemize_attr(value)
    elif type(value) in [list, set, tuple]:
        name = ":" + name

        value = type(value)([to_decimal(v) for v in value])

        return {
            "UpdateExpression": name,
            "ExpressionAttributeNames": {},
            "ExpressionAttributeValues": {name: value},
        }
    else:
        name = ":" + name
        return {
            "UpdateExpression": name,
            "ExpressionAttributeNames": {},
            "ExpressionAttributeValues": {name: to_decimal(value)},
        }


def itemize_attr(attr):
    # Separate indexes
    parts = []

    current_section = ""
    for letter in attr._awstin_name:
        if letter == "[":
            parts.append(current_section)
            current_section = "["
        elif letter == "]":
            parts.append(current_section + "]")
            current_section = ""
        else:
            current_section += letter
    if current_section:
        parts.append(current_section)

    serialized = ""
    name_map = {}

    # Separate attributes
    for part in parts:
        if "[" in part and "]" in part:
            serialized += part
        else:
            if part.startswith("."):
                serialized += "."
                part = part[1:]
            sections = part.split(".")
            serialized_sections = []

            for section in sections:
                name = "#" + str(uuid.uuid4())[:8]
                name_map[name] = section
                serialized_sections.append(name)
            serialized += ".".join(serialized_sections)

    result = {
        "UpdateExpression": serialized,
        "ExpressionAttributeNames": name_map,
        "ExpressionAttributeValues": {},
    }
    return result


class UpdateOperand:
    """
    Inner part of an update expression
    """

    def __init__(self, value):
        self.value = value

    def serialize(self):
        return serialize_operand(self.value)


class CombineOperand(UpdateOperand):
    """
    Add or subtact two expressions
    """

    def __init__(self, left, right, symbol):
        self.left = left
        self.right = right
        self.symbol = symbol

    def serialize(self):
        ser_left = serialize_operand(self.left)
        ser_right = serialize_operand(self.right)

        expression = (
            f"{ser_left['UpdateExpression']} "
            f"{self.symbol} "
            f"{ser_right['UpdateExpression']}"
        )

        return {
            "UpdateExpression": expression,
            "ExpressionAttributeNames": dict(
                **ser_left["ExpressionAttributeNames"],
                **ser_right["ExpressionAttributeNames"],
            ),
            "ExpressionAttributeValues": dict(
                **ser_left["ExpressionAttributeValues"],
                **ser_right["ExpressionAttributeValues"],
            ),
        }


class IfNotExistsOperand(UpdateOperand):
    """
    Set a value if the given attribute does not exist
    """

    def __init__(self, attr, value):
        self.attr = attr
        self.value = value

    def serialize(self):
        ser_attr = serialize_operand(self.attr)
        ser_value = serialize_operand(self.value)

        expression = (
            f"if_not_exists({ser_attr['UpdateExpression']}, "
            f"{ser_value['UpdateExpression']})"
        )

        return {
            "UpdateExpression": expression,
            "ExpressionAttributeNames": dict(
                **ser_attr["ExpressionAttributeNames"],
                **ser_value["ExpressionAttributeNames"],
            ),
            "ExpressionAttributeValues": dict(
                **ser_attr["ExpressionAttributeValues"],
                **ser_value["ExpressionAttributeValues"],
            ),
        }


class ListAppendOperand(UpdateOperand):
    """
    Combine two lists
    """

    def __init__(self, left, right):
        self.left = left
        self.right = right

    def serialize(self):
        ser_left = serialize_operand(self.left)
        ser_right = serialize_operand(self.right)

        expression = (
            f"list_append({ser_left['UpdateExpression']}, "
            f"{ser_right['UpdateExpression']})"
        )

        return {
            "UpdateExpression": expression,
            "ExpressionAttributeNames": dict(
                **ser_left["ExpressionAttributeNames"],
                **ser_right["ExpressionAttributeNames"],
            ),
            "ExpressionAttributeValues": dict(
                **ser_left["ExpressionAttributeValues"],
                **ser_right["ExpressionAttributeValues"],
            ),
        }


def list_append(left, right):
    """
    Set a value to the combination of two lists in an update expression
    """
    return ListAppendOperand(UpdateOperand(left), UpdateOperand(right))
