# pylint: disable=W0212
import typing
from typing import Any, TypeVar, Literal, Type, Dict, Set, List
from decimal import Decimal

from boto3.dynamodb.types import TypeSerializer
from pydash import get, last, head
from pydantic import BaseModel
from pydantic.fields import FieldInfo

from dynamantic.main import Dynamantic, T, type_serialize, dynamodb_compatible_value
from dynamantic.exceptions import AttributeTypeInvalidError

E = TypeVar("E", bound="Expr")
# C = TypeVar("C", bound="Condition")
CE = TypeVar("CE", bound="ConditionExpression")
F = TypeVar("F", bound="Field")


# class ConditionExpression:
#     _expr: E
#     _condition: C

#     update_expression: str
#     expression_attribute_values: Dict[str, Dict]

#     def __init__(self, value: Any, condition: Type[C]) -> None:
#         self._condition = condition
#         self._expr = condition._expr

#         if value.__class__ in self._condition._classes:
#             equals = ""
#             if self._expr._action == "SET":
#                 equals = "= "
#             self.update_expression = f"{self._expr._action} {self._expr._key} {equals}{self._condition._operand}"
#             serialized = type_serialize(key=self._condition._key_value, value=value)
#             for k, v in serialized[self._condition._key_value].items():
#                 if k in ("NS", "BS", "SS"):
#                     serialized[self._condition._key_value][k] = list(v)
#             self.expression_attribute_values = serialized
#             return
#         raise AttributeTypeInvalidError(str(value.__class__), str(self._condition._classes))


# class Condition:
#     _expr: E
#     _operand: str
#     _classes: Set[Type]

#     _key_value: str

#     def __init__(self, expression: Type[E]) -> None:
#         self._expr = expression
#         self._classes = expression._cls_model._pydantic_types(self._expr._key)
#         self._key_value = f":{self._expr._key}"

#     def set_append(self, value: list | set) -> ConditionExpression:
#         self._expr._action = "SET" if isinstance(value, list) else "ADD"
#         self._expr._value = value
#         if isinstance(value, list):
#             self._operand = f"list_append({self._expr._key}, {self._key_value})"
#         elif isinstance(value, set):
#             self._operand = self._key_value
#         else:
#             raise AttributeTypeInvalidError(str(value.__class__), str({list, set}))
#         return ConditionExpression(value=value, condition=self)

#     def set_add(self, value: int | float | Decimal) -> ConditionExpression:
#         self._expr._action = "SET"
#         self._expr._value = value
#         if isinstance(value, float):
#             # float must be converted to Decimal and appended to approved classes
#             value = dynamodb_compatible_value(value)
#             self._classes.add(value.__class__)
#         if isinstance(value, (int, Decimal)):
#             self._operand = f"{self._expr._key} + {self._key_value}"
#         else:
#             raise AttributeTypeInvalidError(str(value.__class__), str({int, float, Decimal}))
#         return ConditionExpression(value=value, condition=self)

#     def set(self: Type[E], value: Any) -> ConditionExpression:
#         self._expr._action = "SET"
#         self._expr._value = value
#         self._operand = f"{self._key_value}"
#         return ConditionExpression(value=value, condition=self)

#     def remove(self, value: Any):
#         self._action = "REMOVE"
#         pass

#     def add(self, value: Any):
#         self._action = "ADD"
#         pass

#     def delete(self, value: Any):
#         self._action = "DELETE"
#         pass


# class Expr:
#     _key: str
#     _info: FieldInfo
#     _cls_model: T

#     _action: Literal["SET", "REMOVE", "ADD", "DELETE"]
#     _value: Any

#     def __init__(self, cls_model: Type[T]) -> None:
#         self._cls_model = cls_model

#     def field(self, key: str) -> Condition:
#         if key in self._cls_model.model_fields:
#             self._key = key
#             self._info = self._cls_model.model_fields.get(key)
#             return Condition(self)
#         raise Exception("Update expression key does not exist on model.")


class ConditionExpression:
    _expr: E

    update_expression: str
    expression_attribute_values: Dict[str, Dict]

    def __init__(self, value: Any, expression: Type[E]) -> None:
        self._expr = expression

        equals = ""
        if self._expr._action == "SET":
            equals = "= "

        # Get the key of the last field
        self.update_expression = f"{self._expr._action} {expression._compile()} {equals}{self._expr._operand}"
        key = f":{self._expr._key}"
        print(self.update_expression)

        serialized = type_serialize(key=key, value=value)
        for k, v in serialized[key].items():
            if k in ("NS", "BS", "SS"):
                serialized[key][k] = list(v)
        self.expression_attribute_values = serialized


class Field:
    _expr: E
    _properties: dict

    _key: str | int
    _type: Literal["key", "index"]

    def __init__(self, expression: E, properties=None, key: str | int = None) -> None:
        self._expr = expression
        self._properties = properties

        self._key = key
        if self._is_int(key):
            self._type = "index"
        else:
            self._type = "key"
            self._expr._key = key

        self._expr._fields.append(self)

    def _is_int(self, val):
        try:
            return float(str(val)).is_integer()
        except Exception:
            return False

    def field(self, key: str):
        if key in self._properties:
            return self.__class__(self._expr, self._properties[key], key)

        if "type" in self._properties:
            if self._properties.get("type") == "object":
                return self.__class__(self._expr, key=key)

        if "$ref" in self._properties:
            model_type = self._properties.get("$ref").split("/")[-1]
            nested_model = self._expr._cls_model.model_json_schema().get("$defs").get(model_type)
            props = nested_model.get("properties")
            if key in props:
                return self.__class__(self._expr, props.get(key), key)

        if "anyOf" in self._properties:
            objs = list(filter(lambda fld: fld["type"] == "object", self._properties["anyOf"]))
            refs = list(filter(lambda fld: fld["$ref"], self._properties["anyOf"]))
            if len(refs) > 0:
                # is a nested model
                ref = refs[0]
                model_type = ref.get("$ref").split("/")[-1]
                nested_model = self._expr._cls_model.model_json_schema().get("$defs").get(model_type)
                props = nested_model.get("properties")
                if key in props:
                    return self.__class__(self._expr, props.get("key"), key)
            if len(objs) > 0:
                # is an object/dict
                return self.__class__(self._expr, key=key)

        raise Exception

    def index(self, idx: str | int):
        if self._is_int(idx):
            idx = str(idx)
            if "anyOf" in self._properties:
                # optional
                arrays = list(filter(lambda fld: fld["type"] == "array", self._properties["anyOf"]))
                refs = list(filter(lambda fld: fld.get("items", {}).get("$ref"), self._properties["anyOf"]))
                if len(arrays) > 0:
                    # is an array and works
                    if len(refs) > 0:
                        # of subclasses
                        ref = refs[0]["items"]
                        model_type = ref.get("$ref").split("/")[-1]
                        nested_model = self._expr._cls_model.model_json_schema().get("$defs").get(model_type)
                        props = nested_model.get("properties")
                        return self.__class__(self._expr, props, idx)
                    return self.__class__(self, arrays[0], idx)

            if "type" in self._properties:
                if self._properties["type"] == "array":
                    return self.__class__(self._expr, key=idx)

        raise Exception

    def set(self, value: Any) -> ConditionExpression:
        self._expr._action = "SET"
        self._expr._value = value
        self._expr._operand = f":{self._expr._key}"
        return ConditionExpression(value=value, expression=self._expr)

    def set_add(self, value: int | float | Decimal) -> ConditionExpression:
        self._expr._action = "SET"
        self._expr._value = value
        if isinstance(value, float):
            # float must be converted to Decimal and appended to approved classes
            value = dynamodb_compatible_value(value)
        if isinstance(value, (int, Decimal)):
            self._expr._operand = f"{self._expr._compile()} + :{self._expr._key}"
        else:
            raise AttributeTypeInvalidError(str(value.__class__), str({int, float, Decimal}))
        return ConditionExpression(value=value, expression=self._expr)

    def set_append(self, value: List[Any] | Set[Any]) -> ConditionExpression:
        props = self._expr._properties
        sets = []
        lists = []
        refs = []
        if "anyOf" in props:
            sets = list(filter(lambda fld: fld["type"] == "array" and fld.get("uniqueItems"), props["anyOf"]))
            lists = list(filter(lambda fld: fld["type"] == "array" and not fld.get("uniqueItems"), props["anyOf"]))
            refs = list(filter(lambda fld: fld.get("items", {}).get("$ref"), self._properties["anyOf"]))
        elif props["type"] == "array":
            if props["uniqueItems"]:
                lists.append(props)
            elif props.get("items", {}).get("$ref"):
                refs.append(props)
            else:
                sets.append(props)

        # check for models
        if "$ref" in props:
            refs.append(props)

        self._expr._action = "SET" if isinstance(value, list) else "ADD"
        self._expr._value = value
        if isinstance(value, list) and len(lists) > 0:
            print(refs)
            if len(refs) > 0:
                # need to serialize the model so it can be added
                for x, val in enumerate(value):
                    if issubclass(val.__class__, BaseModel):
                        value[x] = val.model_dump()
            self._expr._operand = f"list_append({self._expr._compile()}, :{self._expr._key})"
        elif isinstance(value, set) and len(sets) > 0:
            # TODO: check type
            self._expr._operand = f":{self._expr._key}"
        else:
            raise AttributeTypeInvalidError(str(value.__class__), str({list, set}))
        return ConditionExpression(value=value, expression=self._expr)


class Expr:
    _cls_model: T

    _fields: List[F]
    _properties: Dict

    _action: Literal["SET", "REMOVE", "ADD", "DELETE"]
    _value: Any
    _key: str

    _operand: str

    def __init__(self, cls_model: Type[T]) -> None:
        self._action = None
        self._value = None
        self._key = None
        self._operand = None
        self._properties = {}
        self._fields = []
        self._cls_model = cls_model

    def field(self, key: str) -> Field:
        props = self._cls_model.model_json_schema().get("properties")
        if props == None:
            raise Exception("Properties do not exist on model")
        if key in props:
            self._properties = props.get(key)
            return Field(self, self._properties, key)
        raise Exception(f"{key} does not exist on model.")

    def _compile(self):
        expr = ""
        for i, field in enumerate(self._fields):
            if field._type == "key":
                expr = expr + "." if i > 0 else expr
                expr = f"{expr}{field._key}"
            else:
                expr = f"{expr}[{field._key}]"
        return expr
