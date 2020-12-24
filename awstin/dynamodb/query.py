class Query:
    def __init__(self, key_expression=None, attr_expression=None):
        self.key_expression = key_expression
        self.attr_expression = attr_expression

    def __and__(self, other: 'Query') -> 'Query':
        result = type(self)()
        if self.key_expression is None:
            result.key_expression = other.key_expression
        elif other.key_expression is None:
            result.key_expression = self.key_expression
        else:
            result.key_expression = self.key_expression & other.key_expression

        if self.attr_expression is None:
            result.attr_expression = other.attr_expression
        elif other.attr_expression is None:
            result.attr_expression = self.attr_expression
        else:
            result.attr_expression = (
                self.attr_expression & other.attr_expression
            )

        return result

    def __or__(self, other: 'Query') -> 'Query':
        result = type(self)()
        if self.key_expression is None:
            result.key_expression = other.key_expression
        elif other.key_expression is None:
            result.key_expression = self.key_expression
        else:
            result.key_expression = self.key_expression | other.key_expression

        if self.attr_expression is None:
            result.attr_expression = other.attr_expression
        elif other.attr_expression is None:
            result.attr_expression = self.attr_expression
        else:
            result.attr_expression = (
                self.attr_expression | other.attr_expression
            )

        return result
