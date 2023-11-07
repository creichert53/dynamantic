# pylint: disable=W0212
import json
from typing import Any, TypeVar, Literal, Type, Dict, Set, List
from decimal import Decimal
import datetime

from pydantic import BaseModel
from boto3.dynamodb.types import Binary

from dynamantic.main import T, type_serialize, dynamodb_compatible_value
from dynamantic.exceptions import AttributeTypeInvalidError, AttributeInvalidError

E = TypeVar("E", bound="Expr")
CE = TypeVar("CE", bound="ConditionExpression")
F = TypeVar("F", bound="Field")


class ConditionExpression:
    _expr: E

    update_expression: str
    expression_attribute_values: Dict[str, Dict]

    def __init__(self, value: Any, expression: Type[E]) -> None:
        self._expr = expression
        self._expression_attribute_values = {}

        self._type_check(value)
        self._create_update_expression(value)

    def _type_check(self, value):
        print(value)
        # allow anything for dict
        if self._expr._properties.get("type") == "object":
            return

        type_mappings = {
            list: self._check_lists_sets,
            set: self._check_lists_sets,
            frozenset: self._check_lists_sets,
            tuple: self._check_lists_sets,
            bool: self._check_boolean,
            (float, Decimal): self._check_number,
            int: self._check_integer,
            str: self._check_string,
            dict: self._check_dict,
            (bytes, bytearray, Binary): self._check_string,
            datetime.datetime: self._check_datetime,
            datetime.date: self._check_date,
            datetime.time: self._check_time,
            BaseModel: self._check_model,
            # Add more type mappings as needed
        }

        for type_, check_function in type_mappings.items():
            if isinstance(value, type_) or issubclass(value.__class__, type_):
                check_function(value)
                return

        raise AttributeTypeInvalidError(str(value.__class__), str(type_mappings.keys()))

    def _check_lists_sets(self, value):
        # Check the type
        props = self._expr._properties
        sets = []
        lists = []
        refs = []
        if "anyOf" in props:
            # look for any sets, list, or model classes in the props
            sets = list(filter(lambda fld: fld["type"] == "array" and fld.get("uniqueItems"), props["anyOf"]))
            lists = list(filter(lambda fld: fld["type"] == "array" and not fld.get("uniqueItems"), props["anyOf"]))
            refs = list(filter(lambda fld: fld.get("items", {}).get("$ref"), self._expr._properties["anyOf"]))
        elif props.get("type") == "array":
            # lists and sets
            if props.get("uniqueItems"):
                sets.append(props)
            else:
                lists.append(props)
            if props.get("items", {}).get("$ref"):
                refs.append(props)

        # Ensure values provide in list works
        if isinstance(value, list) and len(lists) > 0:
            if len(refs) > 0:
                # need to serialize the model so it can be added
                for x, val in enumerate(value):
                    if issubclass(val.__class__, BaseModel):
                        value[x] = val.model_dump()
        if isinstance(value, list) and len(lists) == 0:
            raise AttributeTypeInvalidError(str(value.__class__), str({list}))

        # Ensure set is provided for a type that requires a set
        if isinstance(value, set) and len(sets) == 0:
            raise AttributeTypeInvalidError(str(value.__class__), str({set}))

    def _check_boolean(self, _):
        self._check_type("boolean")

    def _check_number(self, _):
        self._check_type("number")

    def _check_integer(self, _):
        self._check_type("integer")

    def _check_dict(self, _):
        self._check_type("object")

    def _check_string(self, value):
        # Need to check strings against format values that can be cast to strings.
        formats = [val["format"] for val in self._expr._properties.get("anyOf", []) if val.get("format")]
        if self._expr._properties.get("format"):
            formats.append(self._expr._properties["format"])
        for format_ in formats:
            if format_ == "date-time":
                self._check_datetime(value)
            if format_ == "date":
                self._check_date(value)
            if format_ == "time":
                self._check_time(value)
        self._check_type("string")

    def _check_type(self, check_type: Literal["integer", "number", "string", "binary", "boolean", "object"]):
        props = self._expr._properties

        check_vals = [val for val in props.get("anyOf", []) if val.get("type") == check_type]
        if props.get("type") == "array" and props.get("items", {}).get("type") == check_type:
            check_vals.append(props)
        if props.get("type") == check_type:
            check_vals.append(props)
        if len(check_vals) == 0:
            raise AttributeTypeInvalidError(check_type, json.dumps(props))

    def _check_datetime(self, value: str | datetime.datetime):
        try:
            if isinstance(value, datetime.datetime):
                return
            datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError as ve:
            raise AttributeTypeInvalidError("date-time", json.dumps(self._expr._properties)) from ve

    def _check_date(self, value: str | datetime.date):
        try:
            if isinstance(value, datetime.date):
                return
            datetime.date.fromisoformat(value)
        except ValueError as ve:
            raise AttributeTypeInvalidError("date", json.dumps(self._expr._properties)) from ve

    def _check_time(self, value: str | datetime.time):
        try:
            if isinstance(value, datetime.time):
                return
            datetime.datetime.strptime(value, "%H:%M:%S").time()
        except ValueError as ve:
            raise AttributeTypeInvalidError("time", json.dumps(self._expr._properties)) from ve

    def _create_update_expression(self, value: Any):
        equals = ""
        if self._expr._action == "SET":
            equals = "= "

        # Create the update expression string
        self.update_expression = f"{self._expr._action} {self._expr._compile()} {equals}{self._expr._operand}"
        key = f":{self._expr._key}"

        print(value)
        serialized = type_serialize(key=key, value=value)
        print(serialized)
        for k, v in serialized[key].items():
            if k in ("NS", "BS", "SS"):
                serialized[key][k] = list(v)
        self.expression_attribute_values = serialized

    def _check_model(self, value: BaseModel):
        if not issubclass(value.__class__, BaseModel) or (
            issubclass(value.__class__, BaseModel)
            and value.__class__.__name__ in self._expr._properties.get("$ref", "")
        ):
            return
        raise AttributeTypeInvalidError(value.__class__.__name__, json.dumps(self._expr._properties))


class Field:
    _expr: E
    _properties: dict

    _key: str | int
    _type: Literal["key", "index"]

    def __init__(self, expression: E, properties=None, key: str | int = None) -> None:
        self._expr = expression
        self._properties = properties

        if properties:
            self._expr._properties = properties

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
                return self.__class__(self._expr, self._properties, key=key)

        if "$ref" in self._properties:
            model_type = self._properties.get("$ref").split("/")[-1]
            nested_model = self._expr._cls_model.model_json_schema().get("$defs").get(model_type)
            props = nested_model.get("properties")
            if key in props:
                return self.__class__(self._expr, props.get(key), key)

        if "anyOf" in self._properties:
            refs = list(filter(lambda fld: fld.get("$ref"), self._properties.get("anyOf")))
            if len(refs) > 0:
                # is a nested model
                ref = refs[0]
                model_type = ref.get("$ref").split("/")[-1]
                nested_model = self._expr._cls_model.model_json_schema().get("$defs").get(model_type)
                props = nested_model.get("properties")
                if key in props:
                    return self.__class__(self._expr, props.get(key), key)

        raise AttributeInvalidError(name=key)

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
                    return self.__class__(self._expr, arrays[0], idx)

            if "type" in self._properties:
                if self._properties["type"] == "array":
                    return self.__class__(self._expr, key=idx)

        raise AttributeInvalidError(name=str(idx))

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
        self._expr._action = "SET" if isinstance(value, list) else "ADD"
        self._expr._value = value
        if isinstance(value, list):
            self._expr._operand = f"list_append({self._expr._compile()}, :{self._expr._key})"
        elif isinstance(value, set):
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
        if key in props:
            self._properties = props.get(key)
            return Field(self, self._properties, key)
        raise AttributeInvalidError(name=key)

    def _compile(self):
        expr = ""
        for i, field in enumerate(self._fields):
            if field._type == "key":
                expr = expr + "." if i > 0 else expr
                expr = f"{expr}{field._key}"
            else:
                expr = f"{expr}[{field._key}]"
        return expr
