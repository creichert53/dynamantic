import typing
import inspect
from enum import Enum
from decimal import Decimal, ROUND_HALF_UP

from typing import Dict, Any
from datetime import datetime, time, date

from boto3.dynamodb.types import Binary, TypeSerializer
from pydantic import BaseModel


def pydantic_types(cls: BaseModel, key: str, existing: dict = None) -> Dict[str, Any]:
    if existing is None:
        existing = {}

    def traverse(options, full_set: set):
        for option in options:
            if set == typing.get_origin(option):
                full_set.add(set)
            if frozenset == typing.get_origin(option):
                full_set.add(frozenset)
            if list == typing.get_origin(option):
                full_set.add(list)
            if dict == typing.get_origin(option):
                full_set.add(dict)
            if inspect.isclass(option):
                full_set.add(option)
            else:
                traverse(typing.get_args(option), full_set)

    final_set = set()

    type_hint = next(cls.model_fields.get(key_).annotation for key_ in cls.model_fields if key_ == key)

    if inspect.isclass(type_hint):
        final_set.add(type_hint)

    traverse(typing.get_args(type_hint), final_set)

    existing[key] = final_set

    return final_set


def format_float(number: float) -> str:
    dec = Decimal(number)
    dec = dec.quantize(Decimal("0.0000000000"), rounding=ROUND_HALF_UP)
    tup = dec.as_tuple()
    delta = len(tup.digits) + tup.exponent
    digits = "".join(str(d) for d in tup.digits)
    if delta <= 0:
        zeros = abs(tup.exponent) - len(tup.digits)
        val = "0." + ("0" * zeros) + digits
    else:
        val = digits[:delta] + ("0" * tup.exponent) + "." + digits[delta:]
    val = val.rstrip("0")
    if val[-1] == ".":
        val = val[:-1]
    if tup.sign:
        return "-" + val
    return val


def type_serialize(key: str, value) -> Dict[str, Dict[str, Any]]:
    return {key: TypeSerializer().serialize(dynamodb_compatible_value(value))}


def dynamodb_compatible_value(val):
    if isinstance(val, frozenset):
        val = set(val)
    if isinstance(val, set):
        return serialize_map({"set": val})["set"]
    if isinstance(val, Binary):
        return val
    if isinstance(val, float):
        return Decimal(format_float(val))
    if isinstance(val, (datetime, date, time)):
        return val.isoformat()
    if isinstance(val, dict):
        return serialize_map(val)
    if issubclass(val.__class__, BaseModel):
        return val.model_dump()
    return val


def serialize_map(values: dict):
    for k in values.keys():
        v = values[k]
        original = v

        # binary sets fail to deserialize when getting specific attributes in a query.
        # The workaround is to convert to a binary list when writing to the database.
        # It will still be deserialized back to a set upon query.
        is_binary_set = False

        if isinstance(v, float):
            v = dynamodb_compatible_value(v)
        if isinstance(v, (datetime, date, time)):
            v = v.isoformat()
        if isinstance(v, dict):
            serialize_map(v)
        if isinstance(v, (tuple, frozenset)):
            # convert to list, which will be serialized next, converted back at the end
            v = list(v)
        if isinstance(v, set):
            new_set = original.__class__()
            for val in v:
                val = dynamodb_compatible_value(val)
                if isinstance(val, (Binary, bytes, bytearray)):
                    is_binary_set = True
                new_set.add(val)
            # convert to list, which will be serialized next, converted back at the end
            v = list(new_set)
        if isinstance(v, list):
            for x, val in enumerate(v):
                if isinstance(val, (float, datetime, date, time)):
                    v[x] = dynamodb_compatible_value(val)
                if isinstance(val, dict):
                    serialize_map(val)
        if isinstance(original, (tuple, set, frozenset)) and not is_binary_set:
            v = original.__class__(v)
        if isinstance(v, Enum):
            v = v.value
        if issubclass(v.__class__, BaseModel):
            v = serialize_map(v.model_dump())
        values[k] = v
    return values
