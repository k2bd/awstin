from decimal import Decimal
from typing import Union

from boto3.dynamodb.conditions import Attr as BotoAttr, Key as BotoKey


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

    def _convert_value(self, value):
        if isinstance(value, float):
            return Decimal(str(value))
        return value

    @property
    def name(self):
        if self._attribute_name is not None:
            return self._attribute_name
        else:
            return self._name_on_model


class Key(BaseAttribute):
    """
    Used to define and query hash and sort key attributes on a dynamodb table
    data model
    """

    _query_type = BotoKey

    def begins_with(self, value):
        return self._query_type(self.name).begins_with(self._convert_value(value))

    def between(self, low, high):
        return self._query_type(self.name).between(
            self._convert_value(low),
            self._convert_value(high),
        )

    def __eq__(self, value):
        return self._query_type(self.name).eq(self._convert_value(value))

    def __gt__(self, value):
        return self._query_type(self.name).gt(self._convert_value(value))

    def __ge__(self, value):
        return self._query_type(self.name).gte(self._convert_value(value))

    def __lt__(self, value):
        return self._query_type(self.name).lt(self._convert_value(value))

    def __le__(self, value):
        return self._query_type(self.name).lte(self._convert_value(value))


class Attr(Key):
    """
    Used to define and query non-key attributes on a dynamodb table data model
    """

    _query_type = BotoAttr

    def attribute_type(self, value):
        return self._query_type(self.name).attribute_type(
            self._convert_value(value)
        )

    def contains(self, value):
        return self._query_type(self.name).contains(self._convert_value(value))

    def exists(self):
        return self._query_type(self.name).exists()

    def in_(self, value):
        return self._query_type(self.name).is_in(self._convert_value(value))

    def __ne__(self, value):
        return self._query_type(self.name).ne(self._convert_value(value))

    def not_exists(self):
        return self._query_type(self.name).not_exists()


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


class DynamoModel(metaclass=DynamoModelMeta):
    """
    Abstract class for defining DynamoDB data models.

    For example:
    ```python
    class MyDataModel(DynamoModel):
        hashkey_name = Key()

        # The name of the DynamoDB attribute differs from the
        # name on the data model
        sortkey = Key("sortkeyName")

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
                if isinstance(value, Decimal):
                    value = float(value)
                    if int(value) == value:
                        value = int(value)
                setattr(result, model_attrs[db_attr], value)

        return result

    def _to_dynamodb(self):
        model_attrs = type(self)._dynamodb_attributes()

        result = {}

        for dynamo_name, model_name in model_attrs.items():
            value = getattr(self, model_name)
            if value is not NOT_SET:
                if isinstance(value, float):
                    value = Decimal(str(value))
                result[dynamo_name] = value

        return result
