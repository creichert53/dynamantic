import os
import datetime
from uuid import uuid4
from decimal import Decimal
from typing import Any, List, Optional, Set, FrozenSet, Type

from pydantic import Field
from dotenv import load_dotenv

import boto3
from boto3.dynamodb.conditions import Attr as A, Key as K
from dynamantic import Dynamantic, GlobalSecondaryIndex, LocalSecondaryIndex
from dynamantic.main import T

load_dotenv(".env.test")


class MyDeepNestedModel(Dynamantic):
    another_field_float_list: List[bytes]


class MyNestedModel(Dynamantic):
    sample_field: str
    nested_bytes_list: Optional[List[bytes]] = None
    deep_nested: Optional[List[MyDeepNestedModel]] = None


class BaseModel(Dynamantic):
    __table_name__ = os.getenv("DYNAMO_DB_TABLE")
    __table_region__ = os.getenv("AWS_REGION")
    __aws_secret_access_key__ = os.getenv("AWS_SECRET_ACCESS_KEY")
    __aws_access_key_id__ = os.getenv("AWS_ACCESS_KEY_ID")
    __aws_session_token__ = os.getenv("AWS_SECURITY_TOKEN")

    __hash_key__ = "item_id"

    item_id: str = Field(default_factory=lambda: "test:" + str(uuid4()))
    relation_id: str | None = None

    my_simple_bool: bool
    my_simple_bytes: bytes
    my_simple_str: str

    my_tuple: Optional[tuple] = None
    my_frozenset: Optional[FrozenSet[float]] = None
    # my_enum: enum.Enum
    # my_int_enum: enum.IntEnum
    # my_named_tuple: NamedTuple
    # my_typed_dict: TypedDict

    my_int: int | None = None
    my_float: float | None = None
    my_str: Optional[str] = None
    my_bytes: Optional[bytes] = None
    my_bool: Optional[bool] = None
    my_datetime: Optional[datetime.datetime] = None
    my_date: Optional[datetime.date] = None
    my_time: Optional[datetime.time] = None
    # # my_timedelta: Optional[datetime.timedelta] = None
    my_decimal: Optional[Decimal] = None
    my_str_set: Optional[Set[str]] = None
    my_bytes_set: Optional[Set[bytes]] = None
    my_int_set: Optional[Set[int]] = None
    my_float_list: Optional[List[float]] = None
    my_str_list: Optional[List[str]] = None
    my_int_list: Optional[List[int]] = None
    my_bool_list: Optional[List[bool]] = None
    my_dict: Optional[dict] = None
    my_nested_data: Optional[Any] = None
    my_nested_model: Optional[MyNestedModel] = None
    my_nested_model_list: Optional[List[MyNestedModel]] = None


class LocalHostModel(BaseModel):
    __table_host__ = os.getenv("DYNAMO_DB_HOST")


class RangeKeyModel(BaseModel):
    __table_host__ = "http://localhost:8000"
    __range_key__ = "relation_id"


GSI = GlobalSecondaryIndex(
    index_name=os.getenv("DYNAMO_DB_TABLE") + "-gsi",
    hash_key="my_str",
    range_key="relation_id",
    # throughput will use default
)

LSI = LocalSecondaryIndex(
    index_name=os.getenv("DYNAMO_DB_TABLE") + "-lsi",
    hash_key="my_str",
    range_key="relation_id",
)


class GSIModel(RangeKeyModel):
    __gsi__ = [GSI]


class LSIModel(RangeKeyModel):
    __lsi__ = [LSI]


def _create_item_raw(DynamanticModel: Type[T] = None, **kwargs) -> T:
    data = {
        "my_simple_bool": True,
        "my_simple_bytes": b"foo",
        "my_simple_str": "foo",
        "my_tuple": (2.5, "foobar"),
        "my_frozenset": frozenset({2.0, 3.0, 4.0}),
        "my_int": 5,
        "my_float": 2.5,
        "my_str": "bar",
        "my_bytes": b"bar",
        "my_bool": False,
        "my_datetime": datetime.datetime.utcnow(),
        "my_date": datetime.date.today(),
        "my_time": datetime.time(0, 0, 0),
        "my_decimal": Decimal("1.5"),
        "my_str_set": {"a", "b", "c"},
        "my_bytes_set": {b"a", b"b", b"c"},
        "my_int_set": {1, 2},
        "my_float_list": [1.0, 2.0],
        "my_str_list": ["a", "b", "c", "d"],
        "my_int_list": [10, 20, 30],
        "my_bool_list": [True, False],
        "my_dict": {"a": 1, "b": 2, "c": 3},
        "my_nested_data": [{"a": [{"foo": "bar"}], "b": "test"}, "some_string"],
        "my_nested_model": MyNestedModel(sample_field="hello"),
        "my_nested_model_list": [
            MyNestedModel(sample_field="world"),
            MyNestedModel(
                sample_field="foobar",
                nested_bytes_list=[b"foo", b"bar"],
                deep_nested=[MyDeepNestedModel(another_field_float_list=[b"hello"])],
            ),
        ],
    }
    if DynamanticModel:
        data.update(kwargs)
        return DynamanticModel(**data)
    else:
        return BaseModel(**data)


def _create_item(DynamanticModel: Type[T], **kwargs) -> T:
    tables = DynamanticModel._dynamodb().list_tables()["TableNames"]
    if DynamanticModel.__table_name__ not in tables:
        print("TABLE DOESNT EXIST")
        DynamanticModel.create_table()
    else:
        print("TABLE DOES EXIST")
    return _create_item_raw(DynamanticModel, **kwargs)


def _save_items(DynamanticModel, add_count: int = None):
    item1: DynamanticModel = _create_item(
        DynamanticModel,
        item_id="hello:world",
        relation_id="relation_id:hello:world",
        my_str="item1",
    )
    item1.save()

    item2: DynamanticModel = _create_item(
        DynamanticModel,
        item_id="foo:bar",
        relation_id="relation_id:foo:bar",
        my_str="item2",
    )
    item2.save()

    item3: DynamanticModel = _create_item(
        DynamanticModel,
        item_id="hello:world",
        relation_id="relation_id:foo:bar",
        my_str="item2",
    )
    item3.save()

    if add_count is not None:
        for x in range(add_count):
            item: DynamanticModel = _create_item(
                DynamanticModel,
                item_id="hello:world",
                relation_id="relation_id:" + str(uuid4()),
                my_int=x,
            )
            item.save()


# _save_items(RangeKeyModel, add_count=3)
results = RangeKeyModel.query(
    "hello:world",
    range_key_condition=K("relation_id").begins_with("relation_id:"),
    filter_condition=A("my_int").eq(2),
    attributes_to_get=["my_int", "my_str"],
)
item_ = next(iter(results))
print(item_)
