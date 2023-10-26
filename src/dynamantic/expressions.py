# pylint: disable=W0212
from typing import Any, TypeVar, Literal, Type, Dict, Set
from decimal import Decimal

from pydantic.fields import FieldInfo

from dynamantic.main import T, type_serialize, dynamodb_compatible_value
from dynamantic.exceptions import AttributeTypeInvalidError

E = TypeVar("E", bound="Expr")
C = TypeVar("C", bound="Condition")
CE = TypeVar("CE", bound="ConditionExpression")


class ConditionExpression:
    _expr: E
    _condition: C

    update_expression: str
    expression_attribute_values: Dict[str, Dict]

    def __init__(self, value: Any, condition: Type[C]) -> None:
        self._condition = condition
        self._expr = condition._expr

        if value.__class__ in self._condition._classes:
            self.update_expression = f"{self._expr._action} {self._expr._key} = {self._condition._operand}"
            serialized = type_serialize(key=self._condition._key_value, value=value)
            for k, v in serialized[self._condition._key_value].items():
                if k in ("NS", "BS", "SS"):
                    serialized[self._condition._key_value][k] = list(v)
            self.expression_attribute_values = serialized
            return
        raise AttributeTypeInvalidError(str(value.__class__), str(self._condition._classes))


class Condition:
    _expr: E
    _operand: str
    _classes: Set[Type]

    _key_value: str

    def __init__(self, expression: Type[E]) -> None:
        self._expr = expression
        self._classes = expression._cls_model._pydantic_types(self._expr._key)
        self._key_value = f":{self._expr._key}"

    def set_append(self, value: list | set) -> ConditionExpression:
        self._expr._action = "SET"
        self._expr._value = value
        if isinstance(value, set):
            value = list(value)
            self._classes.add(list)
        if isinstance(value, (list, set)):
            self._operand = f"list_append({self._expr._key}, {self._key_value})"
        else:
            raise AttributeTypeInvalidError(str(value.__class__), str({list, set}))
        return ConditionExpression(value=value, condition=self)

    def set_add(self, value: int | float | Decimal) -> ConditionExpression:
        self._expr._action = "SET"
        self._expr._value = value
        if isinstance(value, float):
            # float must be converted to Decimal and appended to approved classes
            value = dynamodb_compatible_value(value)
            self._classes.add(value.__class__)
        if isinstance(value, (int, Decimal)):
            self._operand = f"{self._expr._key} + {self._key_value}"
        else:
            raise AttributeTypeInvalidError(str(value.__class__), str({int, float, Decimal}))
        return ConditionExpression(value=value, condition=self)

    def set(self: Type[E], value: Any) -> ConditionExpression:
        self._expr._action = "SET"
        self._expr._value = value
        self._operand = f"{self._key_value}"
        return ConditionExpression(value=value, condition=self)

    # def remove(self, value: Any):
    #     self._action = "REMOVE"
    #     pass

    # def add(self, value: Any):
    #     self._action = "ADD"
    #     pass

    # def delete(self, value: Any):
    #     self._action = "DELETE"
    #     pass


class Expr:
    _key: str
    _info: FieldInfo
    _cls_model: T

    _action: Literal["SET", "REMOVE", "ADD", "DELETE"]
    _value: Any

    def __init__(self, cls_model: Type[T]) -> None:
        self._cls_model = cls_model

    def field(self, key: str) -> Condition:
        if key in self._cls_model.model_fields:
            self._key = key
            self._info = self._cls_model.model_fields.get(key)
            return Condition(self)
        raise Exception("Update expression key does not exist on model.")
