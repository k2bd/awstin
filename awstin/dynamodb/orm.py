from typing import Union
from boto3.dynamodb.conditions import Attr as BotoAttr, Key as BotoKey

from awstin.dynamodb.query import Query

NOT_SET = object()


class BaseAttribute:
    def __init__(self, attribute_name: Union[str, None] = None):
        # Set by user
        self._attribute_name = attribute_name

        # Set by Model
        self._name_on_model = None
        self._table_name = None

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

    def _make_query(self, boto_query):
        return Query(key_expression=boto_query)

    def begins_with(self, value):
        return self._make_query(self.query_type(self.name).begins_with(value))

    def between(self, low, high):
        return self._make_query(self.query_type(self.name).between(low, high))

    def __eq__(self, value):
        return self._make_query(self._query_type(self.name).eq(value))

    def __gt__(self, value):
        return self._make_query(self._query_type(self.name).gt(value))

    def __ge__(self, value):
        return self._make_query(self._query_type(self.name).gte(value))

    def __lt__(self, value):
        return self._make_query(self._query_type(self.name).lt(value))

    def __le__(self, value):
        return self._make_query(self._query_type(self.name).lte(value))


class Attr(Key):
    """
    Used to define and query non-key attributes on a dynamodb table data model
    """

    _query_type = BotoAttr

    def _make_query(self, boto_query):
        return Query(attr_expression=boto_query)

    def attribute_type(self, value):
        return self._make_query(
            self._query_type(self.name).attribute_type(value)
        )

    def contains(self, value):
        return self._make_query(self._query_type(self.name).contains(value))

    def exists(self):
        return self._make_query(self._query_type(self.name).exists())

    def in_(self, value):
        return self._make_query(self._query_type(self.name).is_in(value))

    def __ne__(self, value):
        return self._make_query(self._query_type(self.name).ne(value))

    def not_exists(self):
        return self._make_query(self._query_type(self.name).not_exists())


class ModelMeta(type):
    def __getattribute__(self, name):
        attr = super().__getattribute__(name)
        if isinstance(attr, BaseAttribute):
            attr._name_on_model = name
            attr._table_name = self._table_name_
            return attr
        else:
            return attr

    def _dynamodb_attributes(self):
        result = {
            attr.name: attr
            for attr in dir(self)
            if isinstance(attr, BaseAttribute)
        }
        print(result)
        return result


class DynamoDBModel(metaclass=ModelMeta):
    @classmethod
    def _from_dynamodb(cls, data):
        model_attrs = cls._dynamodb_attributes()

        result = cls()

        for attr in model_attrs:
            setattr(result, attr, NOT_SET)

        for db_attr, value in data.items():
            if db_attr in model_attrs.keys():
                setattr(result, model_attrs[db_attr], value)

        return result
