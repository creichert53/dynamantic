import datetime
from decimal import Decimal
from tests.conftest import BaseModel, MyNestedModel


def test_pydantic_serialize(model_instance: BaseModel):
    serialized = model_instance.serialize()
    assert isinstance(serialized, dict)


#
# ████████╗██╗   ██╗██████╗ ███████╗███████╗
# ╚══██╔══╝╚██╗ ██╔╝██╔══██╗██╔════╝██╔════╝
#   ██║    ╚████╔╝ ██████╔╝█████╗  ███████╗
#   ██║     ╚██╔╝  ██╔═══╝ ██╔══╝  ╚════██║
#   ██║      ██║   ██║     ███████╗███████║
#   ╚═╝      ╚═╝   ╚═╝     ╚══════╝╚══════╝
#
#


def test_pydantic_type_str_list():
    classes = BaseModel._pydantic_types("my_str_list")
    assert str in classes
    assert list in classes


def test_pydantic_type_str_set():
    classes = BaseModel._pydantic_types("my_str_set")
    assert str in classes
    assert set in classes


def test_pydantic_type_decimal():
    assert Decimal in BaseModel._pydantic_types("my_decimal")


#
# ███████╗███████╗██████╗ ██╗ █████╗ ██╗     ██╗███████╗███████╗██████╗
# ██╔════╝██╔════╝██╔══██╗██║██╔══██╗██║     ██║╚══███╔╝██╔════╝██╔══██╗
# ███████╗█████╗  ██████╔╝██║███████║██║     ██║  ███╔╝ █████╗  ██████╔╝
# ╚════██║██╔══╝  ██╔══██╗██║██╔══██║██║     ██║ ███╔╝  ██╔══╝  ██╔══██╗
# ███████║███████╗██║  ██║██║██║  ██║███████╗██║███████╗███████╗██║  ██║
# ╚══════╝╚══════╝╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚══════╝╚═╝╚══════╝╚══════╝╚═╝  ╚═╝
#
#


# my_tuple: tuple
def test_serializer_tuple(serialized):
    assert serialized.get("my_tuple").__class__ == tuple


# my_frozenset: frozenset
def test_serializer_frozenset(serialized):
    assert serialized.get("my_frozenset").__class__ == frozenset


# my_enum: enum.Enum
# my_int_enum: enum.IntEnum
# my_named_tuple: NamedTuple
# my_typed_dict: TypedDict


# my_simple_bool: bool
def test_serializer_bool(serialized):
    assert serialized.get("my_simple_bool").__class__ == bool


# my_simple_bytes: bytes# my_simple_bool: bool
def test_serializer_bytes(serialized):
    assert serialized.get("my_simple_bytes").__class__ == bytes


# my_simple_str: str
def test_serializer_str(serialized):
    assert serialized.get("my_simple_str").__class__ == str


# my_int: int | None = None
def test_serializer_optional_int(serialized):
    assert serialized.get("my_int").__class__ == int


# my_float: float | None = None
def test_serializer_optional_float(serialized):
    assert serialized.get("my_float").__class__ == Decimal


# my_str: Optional[str] = None
def test_serializer_optional_str(serialized):
    assert serialized.get("my_str").__class__ == str


# my_bytes: Optional[bytes] = None
def test_serializer_optional_bytes(serialized):
    assert serialized.get("my_bytes").__class__ == bytes


# my_bool: Optional[bool] = None
def test_serializer_optional_bool(serialized):
    assert serialized.get("my_bool").__class__ == bool


# my_datetime: Optional[datetime.datetime] = None
def test_serializer_optional_datetime(serialized):
    assert serialized.get("my_datetime").__class__ == str


# my_date: Optional[datetime.date] = None
def test_serializer_optional_date(serialized):
    assert serialized.get("my_date").__class__ == str


# my_time: Optional[datetime.time] = None
def test_serializer_optional_time(serialized):
    assert serialized.get("my_time").__class__ == str


# my_decimal: Optional[Decimal] = None
def test_serializer_optional_decimal(serialized):
    assert serialized.get("my_decimal").__class__ == Decimal


# my_str_set: Optional[Set[str]] = None
def test_serializer_optional_str_set(serialized):
    assert serialized.get("my_str_set").__class__ == set
    assert next(iter(serialized.get("my_str_set"))).__class__ == str


# my_bytes_set: Optional[Set[bytes]] = None
def test_serializer_optional_bytes_set(serialized):
    assert serialized.get("my_bytes_set").__class__ == set
    assert next(iter(serialized.get("my_bytes_set"))).__class__ == bytes


# my_int_set: Optional[Set[int]] = None
def test_serializer_optional_int_set(serialized):
    assert serialized.get("my_int_set").__class__ == set
    assert next(iter(serialized.get("my_int_set"))).__class__ == int


# my_float_list: Optional[List[float]] = None
def test_serializer_optional_float_list(serialized):
    assert serialized.get("my_float_list").__class__ == list
    assert serialized.get("my_float_list")[0].__class__ == Decimal


# my_str_list: Optional[List[str]] = None
def test_serializer_optional_str_list(serialized):
    assert serialized.get("my_str_list").__class__ == list
    assert serialized.get("my_str_list")[0].__class__ == str


# my_int_list: Optional[List[int]] = None
def test_serializer_optional_int_list(serialized):
    assert serialized.get("my_int_list").__class__ == list
    assert serialized.get("my_int_list")[0].__class__ == int


# my_bool_list: Optional[List[bool]] = None
def test_serializer_optional_bool_list(serialized):
    assert serialized.get("my_bool_list").__class__ == list
    assert serialized.get("my_bool_list")[0].__class__ == bool


# my_dict: Optional[dict] = None
def test_serializer_optional_dict(serialized):
    assert serialized.get("my_dict").__class__ == dict


# my_nested_model: Optional[MyNestedModel] = None
def test_serializer_optional_nested_model(serialized):
    assert serialized.get("my_nested_model").__class__ == dict


# my_nested_model_list: Optional[List[MyNestedModel]] = None
def test_serializer_optional_nested_model_list(serialized):
    assert serialized.get("my_nested_model_list").__class__ == list
    assert serialized.get("my_nested_model_list")[0].__class__ == dict


#
# ██████╗ ███████╗███████╗███████╗██████╗ ██╗ █████╗ ██╗     ██╗███████╗███████╗██████╗
# ██╔══██╗██╔════╝██╔════╝██╔════╝██╔══██╗██║██╔══██╗██║     ██║╚══███╔╝██╔════╝██╔══██╗
# ██║  ██║█████╗  ███████╗█████╗  ██████╔╝██║███████║██║     ██║  ███╔╝ █████╗  ██████╔╝
# ██║  ██║██╔══╝  ╚════██║██╔══╝  ██╔══██╗██║██╔══██║██║     ██║ ███╔╝  ██╔══╝  ██╔══██╗
# ██████╔╝███████╗███████║███████╗██║  ██║██║██║  ██║███████╗██║███████╗███████╗██║  ██║
# ╚═════╝ ╚══════╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚══════╝╚═╝╚══════╝╚══════╝╚═╝  ╚═╝
#
#


# my_tuple: tuple
def test_deserialzer_tuple(deserialized: BaseModel):
    assert deserialized.my_tuple.__class__ == tuple


# my_frozenset: frozenset
def test_deserialzer_frozenset(deserialized: BaseModel):
    assert deserialized.my_frozenset.__class__ == frozenset


# my_simple_bool: bool
def test_deserialzer_bool(deserialized: BaseModel):
    assert deserialized.my_simple_bool.__class__ == bool


# my_simple_bytes: bytes# my_simple_bool: bool
def test_deserialzer_bytes(deserialized: BaseModel):
    assert deserialized.my_simple_bytes.__class__ == bytes


# my_simple_str: str
def test_deserialzer_str(deserialized: BaseModel):
    assert deserialized.my_simple_str.__class__ == str


# my_int: int | None = None
def test_deserialzer_optional_int(deserialized: BaseModel):
    assert deserialized.my_int.__class__ == int


# my_float: float | None = None
def test_deserialzer_optional_float(deserialized: BaseModel):
    assert deserialized.my_float.__class__ == float


# my_str: Optional[str] = None
def test_deserialzer_optional_str(deserialized: BaseModel):
    assert deserialized.my_str.__class__ == str


# my_bytes: Optional[bytes] = None
def test_deserialzer_optional_bytes(deserialized: BaseModel):
    assert deserialized.my_bytes.__class__ == bytes


# my_bool: Optional[bool] = None
def test_deserialzer_optional_bool(deserialized: BaseModel):
    assert deserialized.my_bool.__class__ == bool


# my_datetime: Optional[datetime.datetime] = None
def test_deserialzer_optional_datetime(deserialized: BaseModel):
    assert deserialized.my_datetime.__class__ == datetime.datetime


# my_date: Optional[datetime.date] = None
def test_deserialzer_optional_date(deserialized: BaseModel):
    assert deserialized.my_date.__class__ == datetime.date


# my_time: Optional[datetime.time] = None
def test_deserialzer_optional_time(deserialized: BaseModel):
    assert deserialized.my_time.__class__ == datetime.time


# my_decimal: Optional[Decimal] = None
def test_deserialzer_optional_decimal(deserialized: BaseModel):
    assert deserialized.my_decimal.__class__ == Decimal


# my_str_set: Optional[Set[str]] = None
def test_deserialzer_optional_str_set(deserialized: BaseModel):
    assert deserialized.my_str_set.__class__ == set
    assert next(iter(deserialized.my_str_set)).__class__ == str


# my_bytes_set: Optional[Set[bytes]] = None
def test_deserialzer_optional_bytes_set(deserialized: BaseModel):
    assert deserialized.my_bytes_set.__class__ == set
    assert next(iter(deserialized.my_bytes_set)).__class__ == bytes


# my_int_set: Optional[Set[int]] = None
def test_deserialzer_optional_int_set(deserialized: BaseModel):
    assert deserialized.my_int_set.__class__ == set
    assert next(iter(deserialized.my_int_set)).__class__ == int


# my_float_list: Optional[List[float]] = None
def test_deserialzer_optional_float_list(deserialized: BaseModel):
    assert deserialized.my_float_list.__class__ == list
    assert deserialized.my_float_list[0].__class__ == float


# my_str_list: Optional[List[str]] = None
def test_deserialzer_optional_str_list(deserialized: BaseModel):
    assert deserialized.my_str_list.__class__ == list
    assert deserialized.my_str_list[0].__class__ == str


# my_int_list: Optional[List[int]] = None
def test_deserialzer_optional_int_list(deserialized: BaseModel):
    assert deserialized.my_int_list.__class__ == list
    assert deserialized.my_int_list[0].__class__ == int


# my_bool_list: Optional[List[bool]] = None
def test_deserialzer_optional_bool_list(deserialized: BaseModel):
    assert deserialized.my_bool_list.__class__ == list
    assert deserialized.my_bool_list[0].__class__ == bool


# my_dict: Optional[dict] = None
def test_deserialzer_optional_dict(deserialized: BaseModel):
    assert deserialized.my_dict.__class__ == dict


# my_nested_model: Optional[MyNestedModel] = None
def test_deserialzer_optional_nested_model(deserialized: BaseModel):
    assert deserialized.my_nested_model.__class__ == MyNestedModel


# my_nested_model_list: Optional[List[MyNestedModel]] = None
def test_deserialzer_optional_nested_model_list(deserialized: BaseModel):
    assert deserialized.my_nested_model_list.__class__ == list
    assert deserialized.my_nested_model_list[0].__class__ == MyNestedModel


#
# ██████╗ ██╗   ██╗███╗   ██╗ █████╗ ███╗   ███╗ ██████╗ ██████╗ ██████╗     ████████╗██╗   ██╗██████╗ ███████╗███████╗
# ██╔══██╗╚██╗ ██╔╝████╗  ██║██╔══██╗████╗ ████║██╔═══██╗██╔══██╗██╔══██╗    ╚══██╔══╝╚██╗ ██╔╝██╔══██╗██╔════╝██╔════╝
# ██║  ██║ ╚████╔╝ ██╔██╗ ██║███████║██╔████╔██║██║   ██║██║  ██║██████╔╝       ██║    ╚████╔╝ ██████╔╝█████╗  ███████╗
# ██║  ██║  ╚██╔╝  ██║╚██╗██║██╔══██║██║╚██╔╝██║██║   ██║██║  ██║██╔══██╗       ██║     ╚██╔╝  ██╔═══╝ ██╔══╝  ╚════██║
# ██████╔╝   ██║   ██║ ╚████║██║  ██║██║ ╚═╝ ██║╚██████╔╝██████╔╝██████╔╝       ██║      ██║   ██║     ███████╗███████║
# ╚═════╝    ╚═╝   ╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝     ╚═╝ ╚═════╝ ╚═════╝ ╚═════╝        ╚═╝      ╚═╝   ╚═╝     ╚══════╝╚══════╝
#
#


# HASH KEY
def test_dynamodb_type_item_id():
    assert BaseModel._dynamodb_type("item_id") == "S"


# REQUIRED (i.e. single length types)
def test_dynamodb_type_simple_str():
    assert BaseModel._dynamodb_type("my_simple_str") == "S"


def test_dynamodb_type_simple_bool():
    assert BaseModel._dynamodb_type("my_simple_bool") == "BOOL"


# OPTIONAL (i.e. multiple args)
def test_dynamodb_type_str():
    assert BaseModel._dynamodb_type("my_str") == "S"


def test_dynamodb_type_bytes():
    assert BaseModel._dynamodb_type("my_bytes") == "B"


def test_dynamodb_type_bool():
    assert BaseModel._dynamodb_type("my_bool") == "BOOL"


def test_dynamodb_type_int():
    assert BaseModel._dynamodb_type("my_int") == "N"


def test_dynamodb_type_float():
    assert BaseModel._dynamodb_type("my_float") == "N"


def test_dynamodb_type_decimal():
    assert BaseModel._dynamodb_type("my_decimal") == "N"


# SETS
def test_dynamodb_type_str_set():
    assert BaseModel._dynamodb_type("my_str_set") == "SS"


def test_dynamodb_type_bytes_set():
    assert BaseModel._dynamodb_type("my_bytes_set") == "BS"


def test_dynamodb_type_int_set():
    assert BaseModel._dynamodb_type("my_int_set") == "NS"


# LISTS
def test_dynamodb_type_str_list():
    assert BaseModel._dynamodb_type("my_str_list") == "L"


def test_dynamodb_type_int_list():
    assert BaseModel._dynamodb_type("my_int_list") == "L"


def test_dynamodb_type_bool_list():
    assert BaseModel._dynamodb_type("my_bool_list") == "L"


# MAP
def test_dynamodb_type_map():
    assert BaseModel._dynamodb_type("my_dict") == "M"


# CUSTOM
def test_dynamodb_type_datetime():
    assert BaseModel._dynamodb_type("my_datetime") == "S"
