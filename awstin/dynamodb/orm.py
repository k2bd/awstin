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


class Key(BaseAttribute):
    """
    Used to define and query hash and sort key attributes on a dynamodb table
    data model
    """

    _query_type = BotoKey

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


class Attr(Key):
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


class DynamoModel(metaclass=DynamoModelMeta):
    """
    Abstract class for defining DynamoDB data models.

    For example:
    ```python
    class MyDataModel(DynamoModel):
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
