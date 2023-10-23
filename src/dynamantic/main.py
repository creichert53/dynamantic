from decimal import Decimal


import inspect
import typing
from typing import Callable, List, Literal, Set, Type, Dict
from datetime import datetime, time, date

import boto3
from boto3.dynamodb.types import Binary
from boto3.dynamodb.conditions import (
    ComparisonCondition,
    ConditionExpressionBuilder,
    ConditionBase,
)
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

from .attrs import K
from .indexes import LocalSecondaryIndex, GlobalSecondaryIndex
from .exceptions import PutError, GetError, TableError, InvalidStateError

BOTOCORE_EXCEPTIONS = (BotoCoreError, ClientError)


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
            params["ProjectionExpression"] = ", ".join(keys + attributes_to_get)

        return params

    @classmethod
    def scan(
        cls,
        filter_condition: ComparisonCondition | None = None,
        index: GlobalSecondaryIndex | LocalSecondaryIndex | None = None,
        attributes_to_get: List[str] | None = None,
        as_dict: bool = False,
    ):
        params = cls._prepare_operation(
            "", index=index, filter_condition=filter_condition, attributes_to_get=attributes_to_get
        )

        del params["KeyConditionExpression"]

        result: ScanOutputTypeDef = cls._dynamodb_table().scan(**params)
        items = [cls._return_value(item, as_dict) for item in result["Items"]]
        return items

    @classmethod
    def query(
        cls,
        value: str,
        index: GlobalSecondaryIndex | LocalSecondaryIndex | None = None,
        range_key_condition: ComparisonCondition | None = None,
        filter_condition: ComparisonCondition | None = None,
        attributes_to_get: List[str] | None = None,
        as_dict: bool = False,
    ):
        params = cls._prepare_operation(value, index, range_key_condition, filter_condition, attributes_to_get)
        result: QueryOutputTableTypeDef = cls._dynamodb_table().query(**params)
        items = [cls._return_value(item, as_dict) for item in result["Items"]]
        return items

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
    def get(cls, hash_key: str, range_key: str | None = None, as_dict: bool = False):
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

        return cls._return_value(item, as_dict)

    def refresh(self):
        item = self.model_dump()
        item = (
            self.get(item[self.__hash_key__], item[self.__range_key__])
            if self.__range_key__
            else self.get(item[self.__hash_key__])
        )
        # item = self.deserialize(item)
        self.__dict__.update(item)

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
    def delete_table(cls):
        cls._dynamodb_table().delete()

    @classmethod
    def _return_value(cls, item: dict, as_dict: bool = False):
        return cls.deserialize(item) if as_dict else cls(**cls.deserialize(item))

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
        # pylint: disable=R0912
        def _dynamo_type(val):
            if isinstance(val, Binary):
                return Binary(val)
            if isinstance(val, float):
                return Decimal(val)
            if isinstance(val, (datetime, date, time)):
                return val.isoformat()
            if isinstance(val, dict):
                return _serialize_map(val)
            return val

        def _serialize_map(values: dict):
            for k in values.keys():
                v = values[k]
                if isinstance(v, float):
                    v = Decimal(v)
                if isinstance(v, (datetime, date, time)):
                    v = v.isoformat()
                if isinstance(v, dict):
                    _serialize_map(v)
                if isinstance(v, (tuple, frozenset)):
                    # convert to list, which will be serialized next
                    v = list(v)
                if isinstance(v, set):
                    new_set = set()
                    for val in v:
                        new_set.add(_dynamo_type(val))
                    # convert to list, which will be serialized next
                    v = list(new_set)
                if isinstance(v, list):
                    for x, val in enumerate(v):
                        if isinstance(val, (float, datetime, date, time)):
                            v[x] = _dynamo_type(val)
                        if isinstance(val, dict):
                            _serialize_map(val)
                if isinstance(v, Dynamantic):
                    _serialize_map(v.model_dump())
                values[k] = v

        values = self.model_dump()
        _serialize_map(values)

        return {key: value for key, value in values.items() if value is not None}

    @classmethod
    def deserialize(cls, values: dict) -> "Dynamantic":
        def _create_instance(value, class_, collection_class=None):
            if value == None:
                return value

            try:
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
            except Exception as ex:
                print(ex)
                return value

        def _deserialize_value(value, classes: list):
            if len(classes) == 1:
                return _create_instance(value, classes[0])
            if len(classes) == 2:
                return _create_instance(value, classes[0], classes[1])
            return value

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
