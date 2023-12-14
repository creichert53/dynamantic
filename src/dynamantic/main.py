# pylint: disable=W0212
import json
import inspect
import typing
from typing import Callable, List, Literal, Set, Type, Dict, Any, TypeVar, Generic, Tuple
from decimal import Decimal
from datetime import datetime, time, date

import boto3

from boto3.dynamodb.conditions import (
    ComparisonCondition,
    ConditionExpressionBuilder,
    ConditionBase,
)
from boto3.dynamodb.types import Binary, TypeDeserializer
from botocore.client import ClientError
from botocore.exceptions import BotoCoreError

from pydantic import BaseModel

from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from mypy_boto3_dynamodb.type_defs import (
    GlobalSecondaryIndexTypeDef,
    LocalSecondaryIndexTypeDef,
    QueryInputRequestTypeDef,
    QueryOutputTableTypeDef,
    ScanOutputTypeDef,
)
from mypy_boto3_dynamodb.service_resource import _Table

from dynamantic.attrs import K
from dynamantic.indexes import LocalSecondaryIndex, GlobalSecondaryIndex
from dynamantic.exceptions import (
    UpdateError,
    PutError,
    GetError,
    DeleteError,
    TableError,
    InvalidStateError,
    AttributeTypeInvalidError,
    AttributeInvalidError,
)
from dynamantic.types import serialize_map, type_serialize, dynamodb_compatible_value

BOTOCORE_EXCEPTIONS = (BotoCoreError, ClientError)

T = TypeVar("T", bound="Dynamantic")
M = TypeVar("M", bound="_DynamanticFuture")
E = TypeVar("E", bound="Expr")
CE = TypeVar("CE", bound="ConditionExpression")
F = TypeVar("F", bound="Field")


class _TableMetadata:
    __table_name__: str | Callable[[], str]
    __table_region__: str | None = None
    __table_host__: str | None = None
    __aws_access_key_id__: str | None = None
    __aws_secret_access_key__: str | None = None
    __aws_session_token__: str | None = None

    __hash_key__: str
    __range_key__: str | None = None

    __gsi__: List[GlobalSecondaryIndex] = []
    __lsi__: List[LocalSecondaryIndex] = []

    __write_capacity_units__: int | None = 1
    __read_capacity_units__: int | None = 1

    _dynamodb_rsc: DynamoDBServiceResource | None = None
    _dynamodb_client: DynamoDBClient | None = None


class Dynamantic(_TableMetadata, BaseModel):
    def save(self, condition_expression: ComparisonCondition | None = None):
        payload = {
            "TableName": self.__table_name__,
            "Item": self.serialize(),
        }

        if condition_expression:
            _, names, values = self._build_expression(condition_expression)
            payload["ConditionExpression"] = condition_expression
            payload["ExpressionAttributeNames"] = names
            payload["ExpressionAttributeValues"] = values

        try:
            self._dynamodb_table().put_item(**payload)
            self.refresh()
        except BOTOCORE_EXCEPTIONS as exc:
            self.refresh()
            raise PutError(f"Failed to put item: {exc}", exc) from exc

    @classmethod
    def get(cls: Type[T], hash_key: str, range_key: str | None = None) -> T:
        params = {}
        params[cls.__hash_key__] = hash_key
        if cls.__range_key__:
            params[cls.__range_key__] = range_key

        item = (
            cls._dynamodb_table()
            .get_item(
                TableName=cls.__table_name__,
                Key=params,
            )
            .get("Item", {})
        )
        if item == {}:
            raise GetError("Item doesn't exist.")

        return cls._return_value(item)

    @classmethod
    def batch_get(cls: Type[T], items: List[str] | List[Tuple[str, str]]) -> List[T]:
        all_results: List[T] = []
        chunked = [items[i : i + 25] for i in range(0, len(items), 25)]
        for chunk in chunked:
            if cls.__hash_key__ and cls.__range_key__:
                request = {cls.__table_name__: {"Keys": [cls._key(key[0], key[1]) for key in chunk]}}
            else:
                request = {cls.__table_name__: {"Keys": [cls._key(key) for key in chunk]}}
            results = cls._dynamodb().batch_get_item(RequestItems=request)["Responses"][cls.__table_name__]
            for item in results:
                item = {k: TypeDeserializer().deserialize(v) for k, v in item.items()}
                all_results.append(cls(**cls.deserialize(item)))
        return all_results

    def update(self, actions: List["ConditionExpression"], condition_expression: ComparisonCondition | None = None):
        last_action_type, all_actions, all_attribute_values = self._update(actions)

        payload = {
            "Key": self._key_params(),
            "UpdateExpression": " ".join([last_action_type, ", ".join(all_actions)]),
            "ExpressionAttributeValues": {
                key: TypeDeserializer().deserialize(value) for key, value in all_attribute_values.items()
            },
        }

        if condition_expression:
            payload["ConditionExpression"] = condition_expression

        try:
            self._dynamodb_table().update_item(**payload)
            self.refresh()
        except BOTOCORE_EXCEPTIONS as exc:
            self.refresh()
            raise UpdateError(f"Failed to update item: {exc}", exc) from exc

    def delete(self, condition_expression: ComparisonCondition | None = None):
        payload = {
            "Key": self._key_params(),
        }

        if condition_expression:
            payload["ConditionExpression"] = condition_expression

        try:
            self._dynamodb_table().delete_item(**payload)
        except BOTOCORE_EXCEPTIONS as exc:
            raise DeleteError(f"Failed to delete item: {exc}", exc) from exc

    @classmethod
    def scan(
        cls: Type[T],
        filter_condition: ComparisonCondition | None = None,
        index: GlobalSecondaryIndex | LocalSecondaryIndex | None = None,
        attributes_to_get: List[str] | None = None,
    ) -> List[T]:
        """Perform a scan of DynamoDB.

        Args:
            filter_condition (ComparisonCondition, optional):

                    Filter the scan using a condition expression. Defaults to None.
            index (GlobalSecondaryIndex | LocalSecondaryIndex, optional):
                    Provide an index to scan. Defaults to None.

            attributes_to_get (List[str], optional):
                    List of attributes to get. Any required fields in the model will be returned as well.
                    Defaults to None.

        Returns:
            List[T]: List of model instances.
        """
        params = cls._prepare_operation(
            "", index=index, filter_condition=filter_condition, attributes_to_get=attributes_to_get
        )

        del params["KeyConditionExpression"]

        result: ScanOutputTypeDef = cls._dynamodb_table().scan(**params)
        items = [cls._return_value(item) for item in result["Items"]]
        return items

    @classmethod
    def query(
        cls: Type[T],
        value: str,
        range_key_condition: ComparisonCondition | None = None,
        filter_condition: ComparisonCondition | None = None,
        index: GlobalSecondaryIndex | LocalSecondaryIndex | None = None,
        attributes_to_get: List[str] | None = None,
    ) -> List[T]:
        """Perform a query of DynamoDB.

        Args:
            value (str):
                    The hash key value to query for.
                    The hash key for the table if no index, or the index if provided.

            range_key_condition (ComparisonCondition, optional):
                    The condition expression to use on the range key this is required if the table
                    or index you query on contains a range key. Defaults to None.

            filter_condition (ComparisonCondition, optional):
                    Filter the query using a condition expression. Defaults to None.

            index (GlobalSecondaryIndex | LocalSecondaryIndex, optional):
                    Provide an index to scan. Defaults to None.

            attributes_to_get (List[str], optional):
                    List of attributes to get. Any required fields in the model will
                    be returned as well. Defaults to None.

        Returns:
            List[T]: List of model instances.
        """
        params = cls._prepare_operation(value, index, range_key_condition, filter_condition, attributes_to_get)
        result: QueryOutputTableTypeDef = cls._dynamodb_table().query(**params)
        items = [cls._return_value(item) for item in result["Items"]]
        return items

    def refresh(self: T) -> T:
        """Refresh the model from the database."""
        item = self.model_dump()
        item = (
            self.get(item[self.__hash_key__], item[self.__range_key__])
            if self.__range_key__
            else self.get(item[self.__hash_key__])
        )
        self.from_raw_data(item)
        return self

    @classmethod
    def table_exists(cls):
        tables = cls._dynamodb().list_tables()["TableNames"]
        if cls.__table_name__ in tables:
            return True
        return False

    @classmethod
    def create_table(cls, wait: bool = True):
        # PROVISIONED THROUGHPUT
        throughput = {
            "ReadCapacityUnits": cls.__read_capacity_units__,
            "WriteCapacityUnits": cls.__write_capacity_units__,
        }

        # Initialize Attributes
        attributes = {cls.__hash_key__}

        # KEY SCHEMA
        key_schema = [{"AttributeName": cls.__hash_key__, "KeyType": "HASH"}]
        if cls.__range_key__:
            attributes.add(cls.__range_key__)
            key_schema.append({"AttributeName": cls.__range_key__, "KeyType": "RANGE"})

        # GLOBAL SECONDARY INDEXES
        gsis: List[GlobalSecondaryIndexTypeDef] = []
        if len(cls.__gsi__) > 0:
            gsis = [
                {
                    "IndexName": _gsi.index_name,
                    "KeySchema": _gsi.key_schema,
                    "Projection": _gsi.projection,
                    "ProvisionedThroughput": _gsi.throughput,
                }
                for _gsi in cls.__gsi__
            ]

        # LOCAL SECONDARY INDEXES
        lsis: List[LocalSecondaryIndexTypeDef] = []
        if len(cls.__lsi__) > 0:
            lsis = [
                {
                    "IndexName": _lsi.index_name,
                    "KeySchema": _lsi.key_schema,
                    "Projection": _lsi.projection,
                }
                for _lsi in cls.__lsi__
            ]

        # ATTRIBUTE DEFINITIONS
        for index in [*cls.__gsi__, *cls.__lsi__]:
            if hasattr(index, "hash_key") and index.hash_key:
                attributes.add(index.hash_key)
            if hasattr(index, "range_key") and index.range_key:
                attributes.add(index.range_key)

        attribute_definitions = [
            {"AttributeName": attr, "AttributeType": cls._dynamodb_type(attr)} for attr in attributes
        ]

        # Create Table Attributes
        table = {
            "AttributeDefinitions": attribute_definitions,
            "TableName": cls.__table_name__,
            "KeySchema": key_schema,
            "ProvisionedThroughput": throughput,
        }

        if len(gsis) > 0:
            table["GlobalSecondaryIndexes"] = gsis
        if len(lsis) > 0:
            table["LocalSecondaryIndexes"] = lsis

        try:
            cls._dynamodb().create_table(**table)

            if wait:
                waiter = cls._dynamodb().get_waiter("table_exists")
                waiter.wait(
                    TableName=cls.__table_name__,
                    WaiterConfig={"Delay": 1, "MaxAttempts": 5},
                )
        except BOTOCORE_EXCEPTIONS as exc:
            raise TableError(f"Failed to create table: {exc}", exc) from exc

    @classmethod
    def delete_table(cls) -> None:
        cls._dynamodb_table().delete()

    def from_raw_data(self, item: Dict[str, Any]) -> None:
        self.__dict__.update(item)

    @classmethod
    def _required_fields(cls, blacklist: List[str] = None) -> List[str]:
        """Return the fields required for this model. Optionally, provide other attributes
        to blacklist in the return list.

        Args:
            blacklist (List[str]): List of attributes to leave out of the return list.

        Returns:
            List[str]: List of required fields.
        """
        if blacklist is None:
            blacklist = []
        return [
            name for name, field in cls.model_fields.items() if field if field.is_required() and name not in blacklist
        ]

    @classmethod
    def _prepare_operation(
        cls,
        value: str | None = None,
        index: GlobalSecondaryIndex | LocalSecondaryIndex | None = None,
        range_key_condition: ComparisonCondition | None = None,
        filter_condition: ComparisonCondition | None = None,
        attributes_to_get: List[str] | None = None,
    ):
        params: QueryInputRequestTypeDef = {}

        expression: ComparisonCondition = (
            (K(cls.__hash_key__).eq(value) & range_key_condition)
            if range_key_condition
            else K(cls.__hash_key__).eq(value)
        )

        if index:
            if index in cls.__gsi__ + cls.__lsi__:
                params["IndexName"] = index.index_name
                expression = (
                    (K(index.hash_key).eq(value) & range_key_condition)
                    if range_key_condition
                    else K(index.hash_key).eq(value)
                )
            else:
                raise InvalidStateError("Index provided but index does not exist for model.")

        params["KeyConditionExpression"] = expression

        if filter_condition:
            params["FilterExpression"] = filter_condition

        if attributes_to_get and len(attributes_to_get) > 0:
            keys = [cls.__hash_key__]
            if cls.__range_key__:
                keys.append(cls.__range_key__)
            params["ProjectionExpression"] = ", ".join(keys + cls._required_fields() + attributes_to_get)

        return params

    @classmethod
    def _return_value(cls, item: dict) -> T:
        return cls(**cls.deserialize(item))

    @classmethod
    def _build_expression(cls, condition_expression: ConditionBase):
        # Create a ConditionExpressionBuilder object
        builder = ConditionExpressionBuilder()

        # Generate the condition expression string
        return builder.build_expression(condition_expression)

    @classmethod
    def _dynamodb(cls) -> DynamoDBClient:
        if cls._dynamodb_client is None:
            cls._dynamodb_client = boto3.client(
                "dynamodb",
                region_name=cls.__table_region__,
                endpoint_url=cls.__table_host__,
                aws_access_key_id=cls.__aws_access_key_id__,
                aws_secret_access_key=cls.__aws_secret_access_key__,
                aws_session_token=cls.__aws_session_token__,
            )
        return cls._dynamodb_client

    @classmethod
    def _dynamodb_table(cls) -> _Table:
        if cls._dynamodb_rsc is None:
            cls._dynamodb_rsc = boto3.resource(
                "dynamodb",
                region_name=cls.__table_region__,
                endpoint_url=cls.__table_host__,
                aws_access_key_id=cls.__aws_access_key_id__,
                aws_secret_access_key=cls.__aws_secret_access_key__,
                aws_session_token=cls.__aws_session_token__,
            )
        return cls._dynamodb_rsc.Table(cls.__table_name__)

    @classmethod
    def _pydantic_types(cls, key: str) -> Set[Type]:
        def traverse(options, full_set: set):
            for option in options:
                if frozenset == typing.get_origin(option):
                    full_set.add(frozenset)
                if set == typing.get_origin(option):
                    full_set.add(set)
                if list == typing.get_origin(option):
                    full_set.add(list)
                if dict == typing.get_origin(option):
                    full_set.add(dict)
                if inspect.isclass(option):
                    full_set.add(option)
                else:
                    traverse(typing.get_args(option), full_set)

        type_hint = next(cls.model_fields.get(key_).annotation for key_ in cls.model_fields if key_ == key)

        final_set = set()

        if inspect.isclass(type_hint):
            final_set.add(type_hint)

        traverse(typing.get_args(type_hint), final_set)

        return final_set

    @classmethod
    def _dynamodb_type(cls, key: str) -> Literal["S", "N", "B", "M", "L", "BOOL", "NS", "BS", "SS"]:
        classes = cls._pydantic_types(key)
        if set in classes or frozenset in classes:
            if str in classes:
                return "SS"
            if bytes in classes or bytearray in classes:
                return "BS"
            if float in classes or Decimal in classes or int in classes:
                return "NS"
        if list in classes:
            return "L"
        if dict in classes:
            return "M"
        if float in classes or Decimal in classes or int in classes:
            return "N"
        if bytes in classes or bytearray in classes:
            return "B"
        if bool in classes:
            return "BOOL"

        return "S"

    @classmethod
    def _get_base_class(cls, arg, value=None, args: list = None):
        if arg is not None.__class__:
            if inspect.isclass(arg):
                args.append(arg)
                if arg in (list, set, dict, List, Set, Dict):
                    args.append(arg)
            elif arg == typing.Any:
                args.append(type(value))
            else:
                new_args = typing.get_args(arg)
                for new_arg in new_args:
                    args.extend(cls._get_base_class(new_arg, value, args))
                if typing.get_origin(arg) in (list, set, frozenset, dict):
                    args.append(typing.get_origin(arg))

        return args

    def serialize(self) -> dict:
        values = self.model_dump()
        serialize_map(values)
        return {key: value for key, value in values.items() if value is not None}

    @classmethod
    def deserialize(cls, values: dict) -> Dict[str, Any]:
        def _create_instance(value, class_, collection_class=None):
            # if the class fails to deserialize, it is because the value was changed in DynamoDB
            # and is no longer compatible
            if value == None:
                return value

            if class_ in (datetime, date, time):
                return class_.fromisoformat(value)
            if collection_class in (list, tuple, frozenset):
                new_list = []
                for val in value:
                    new_list.append(_create_instance(val, class_))
                return collection_class(new_list)
            if collection_class == set:
                new_set = set()
                for val in iter(value):
                    new_set.add(_create_instance(val, class_))
                return new_set
            if issubclass(class_, Dynamantic):
                deserialized = class_.deserialize(value)
                return class_(**deserialized)
            return class_(value)

        def _deserialize_value(value, classes: list):
            if len(classes) == 1:
                return _create_instance(value, classes[0])
            return _create_instance(value, classes[0], classes[1])

        def _unique_classes_with_order(class_list):
            seen = set()
            unique_classes = [cls for cls in class_list if cls not in seen and not seen.add(cls)]
            return unique_classes

        for k, v in values.items():
            type_hint = next(cls.model_fields.get(key_).annotation for key_ in cls.model_fields if key_ == k)
            classes = cls._get_base_class(type_hint, v, [])
            unique_classes = _unique_classes_with_order(classes)
            updated_value = _deserialize_value(v, unique_classes)
            values[k] = updated_value
        return values

    @classmethod
    def _update(cls, actions=List["ConditionExpression"]):
        last_action_type = None
        all_actions = []
        all_attribute_values = {}
        for action in actions:
            action_parts = action.update_expression.split(" ")
            action_type = action_parts.pop(0)
            if last_action_type is None:
                last_action_type = action_type
            elif action_type != last_action_type:
                raise UpdateError(f"All actions must be the same. {action_type} != {last_action_type}")
            all_actions.append(" ".join(action_parts))
            all_attribute_values = all_attribute_values | action.expression_attribute_values

        return last_action_type, all_actions, all_attribute_values

    @classmethod
    def _key(cls, hash_key: Any, range_key: Any = None) -> Dict[str, Any]:
        key = {cls.__hash_key__: {cls._dynamodb_type(cls.__hash_key__): hash_key}}
        if range_key:
            key[cls.__range_key__] = {cls._dynamodb_type(cls.__range_key__): range_key}
        return key

    def _key_params(self) -> Dict[str, str | float | int | Decimal | Binary]:
        params = {}
        params[self.__hash_key__] = getattr(self, self.__hash_key__)
        if self.__range_key__:
            params[self.__range_key__] = getattr(self, self.__range_key__)
        return params


class _DynamanticFuture(Generic[T]):
    """
    A placeholder object for a model that does not exist yet
    """

    _model: T
    _model_cls: T
    _resolved: bool

    def __init__(self, model_cls: Type[T]) -> None:
        self._model_cls = model_cls
        self._model: T = None
        self._resolved = False

    def from_raw_data(self, item: Dict[str, Any]) -> None:
        self._model = self._model_cls(**self._model_cls.deserialize(item))
        self._resolved = True

    def model_dump(self) -> Dict[str, Any]:
        return self._model.model_dump()

    def refresh(self) -> T:
        """Return the model instance .

        Returns:
            T: [description]
        """
        return self._model


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
            datetime: self._check_datetime,
            date: self._check_date,
            time: self._check_time,
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

    def _check_datetime(self, value: str | datetime):
        try:
            if isinstance(value, datetime):
                return
            datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError as ve:
            raise AttributeTypeInvalidError("date-time", json.dumps(self._expr._properties)) from ve

    def _check_date(self, value: str | date):
        try:
            if isinstance(value, date):
                return
            date.fromisoformat(value)
        except ValueError as ve:
            raise AttributeTypeInvalidError("date", json.dumps(self._expr._properties)) from ve

    def _check_time(self, value: str | time):
        try:
            if isinstance(value, time):
                return
            datetime.strptime(value, "%H:%M:%S").time()
        except ValueError as ve:
            raise AttributeTypeInvalidError("time", json.dumps(self._expr._properties)) from ve

    def _create_update_expression(self, value: Any):
        equals = ""
        if self._expr._action == "SET":
            equals = "= "

        # Create the update expression string
        self.update_expression = f"{self._expr._action} {self._expr._compile()} {equals}{self._expr._operand}"
        key = f":{self._expr._key}"
        serialized = type_serialize(key=key, value=value)
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
