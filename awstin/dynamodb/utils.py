from decimal import Decimal


def to_decimal(value):
    if isinstance(value, float):
        return Decimal(str(value))
    return value


def from_decimal(value):
    if isinstance(value, Decimal):
        value = float(value)
        if int(value) == value:
            value = int(value)
    return value
