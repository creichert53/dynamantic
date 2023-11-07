import pytest
import datetime

from dynamantic.main import _DynamanticFuture
from dynamantic.transactions import TransactGet, TransactWrite
from dynamantic.exceptions import TransactGetError, UpdateError, AttributeTypeInvalidError, AttributeInvalidError
from dynamantic.expressions import Expr

from tests.conftest import BaseModel, RangeKeyModel, MyNestedModel, MyDeepNestedModel, _save_items, _create_item


def test_transact_get_success(dynamodb):
    f = _DynamanticFuture(model_cls=RangeKeyModel)
    f.refresh()

    _save_items(RangeKeyModel)
    with TransactGet() as transaction:
        item1 = transaction.get(RangeKeyModel, "hello:world", "relation_id:hello:world")
        item2 = transaction.get(RangeKeyModel, "foo:bar", "relation_id:foo:bar")

    assert item1.refresh().my_int == 5
    assert item2.model_dump()["my_int"] == 5


def test_transact_get_success_no_range(dynamodb):
    _save_items(BaseModel)
    with TransactGet() as transaction:
        item1 = transaction.get(BaseModel, "foo:bar")

    assert item1.refresh().my_int == 5


def test_transact_get_fail_dne(dynamodb):
    _save_items(RangeKeyModel)
    with pytest.raises(TransactGetError, match=".* does not exist"):
        with TransactGet() as transaction:
            transaction.get(RangeKeyModel, "foo:bar", "dne")


def test_transact_save_success(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    with TransactWrite() as transaction:
        transaction.save(item)

    assert item.refresh().my_int == 5


def test_transact_delete_success(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.delete(item)

    assert len(RangeKeyModel.scan()) == 0


def test_transact_update_set_single_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    original_my_ = item.my_int
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_int").set(9)])

    item.refresh()
    assert item.my_int == 9
    assert item.my_int != original_my_


def test_transact_update_set_datetime_string_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    original_my_ = item.my_datetime
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_datetime").set("2023-10-31 14:30:00")])

    item.refresh()
    assert item.my_datetime == datetime.datetime(2023, 10, 31, 14, 30)
    assert item.my_datetime != original_my_


def test_transact_update_set_datetime_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    new_datetime = datetime.datetime.now() + datetime.timedelta(days=1)
    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_datetime").set(new_datetime)])

    item.refresh()
    assert item.my_datetime == new_datetime


def test_transact_update_set_date_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    new_date = datetime.date.today() + datetime.timedelta(days=2)
    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_date").set(new_date)])

    item.refresh()
    assert item.my_date == new_date


def test_transact_update_set_time_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    new_time = datetime.time(hour=2, minute=30)
    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_time").set(new_time)])

    item.refresh()
    assert item.my_time == new_time


def test_transact_update_set_multiple_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    original_my_int = item.my_int
    original_my_bool = item.my_bool
    item.save()

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[Expr(RangeKeyModel).field("my_int").set(9), Expr(RangeKeyModel).field("my_bool").set(True)],
        )

    item.refresh()
    assert item.my_int == 9
    assert item.my_bool == True
    assert item.my_int != original_my_int
    assert item.my_bool != original_my_bool


def test_transact_update_set_add_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    original_my_int = item.my_int
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_int").set_add(9)])

    assert item.refresh().my_int == original_my_int + 9


def test_transact_update_set_add_fails(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with pytest.raises(AttributeTypeInvalidError, match=f"{str} input not compatible with .*"):
        with TransactWrite() as transaction:
            transaction.update(item, actions=[Expr(RangeKeyModel).field("my_int").set_add("hello")])


def test_transact_update_set_subtract_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    original_my_float = item.my_float
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_float").set_add(-9.2)])

    assert item.refresh().my_float == pytest.approx(original_my_float - 9.2, rel=0.002)


def test_transact_update_set_int_list_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_int_list").set_append([9])])

    item.refresh()
    assert 9 in item.my_int_list
    assert len(item.my_int_list) == 4


def test_transact_update_set_int_list_fails(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with pytest.raises(AttributeTypeInvalidError, match=f"{int} input not compatible with .*"):
        with TransactWrite() as transaction:
            transaction.update(item, actions=[Expr(RangeKeyModel).field("my_int_list").set_append(9)])


def test_transact_update_set_int_set_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_int_set").set_append({9})])

    item.refresh()
    assert 9 in item.my_int_set
    assert len(item.my_int_set) == 3


def test_transact_update_set_str_set_required_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()
    orig_len = len(item.my_required_str_set)

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_required_str_set").set_append({"hello"})])

    item.refresh()
    assert "hello" in item.my_required_str_set
    assert len(item.my_required_str_set) == orig_len + 1


def test_transact_update_set_frozen_set_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_frozenset").set(frozenset({9.0}))])

    item.refresh()
    assert item.my_frozenset == frozenset({9.0})


def test_transact_update_set_bytes_set_fails(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    # Cannot append bytes to bytes set
    with pytest.raises(Exception):
        with TransactWrite() as transaction:
            transaction.update(item, actions=[Expr(RangeKeyModel).field("my_bytes_set").set_append({b"zebra"})])


def test_transact_update_set_bytes_list_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_bytes_list").set_append([b"zebra"])])

    item.refresh()
    assert b"zebra" in item.my_bytes_list
    assert len(item.my_bytes_list) == 4


def test_transact_update_set_string_set_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_str_set").set_append({"z"})])

    item.refresh()
    assert "z" in item.my_str_set
    assert len(item.my_str_set) == 4


def test_transact_update_append_nested_model_list_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    new_deep_nested = MyDeepNestedModel(another_field_bytes_list=[b"hello"])
    new_model = MyNestedModel(sample_field="foobaz", deep_nested_required=new_deep_nested)

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[Expr(RangeKeyModel).field("my_nested_model_list").set_append([new_model])],
        )

    item.refresh()
    assert item.my_nested_model_list[-1].sample_field == "foobaz"


def test_transact_update_append_requred_nested_model_list_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    new_deep_nested = MyDeepNestedModel(another_field_bytes_list=[b"hello"])
    new_model = MyNestedModel(sample_field="foobaz", deep_nested_required=new_deep_nested)

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[Expr(RangeKeyModel).field("my_required_nested_model_list").set_append([new_model])],
        )

    item.refresh()
    assert item.my_required_nested_model_list[-1].sample_field == "foobaz"


def test_transact_update_set_required_nested_model_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    new_deep_nested = MyDeepNestedModel(another_field_bytes_list=[b"hello"])
    new_model = MyNestedModel(sample_field="foobaz", deep_nested_required=new_deep_nested)

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[Expr(RangeKeyModel).field("my_required_nested_model").set(new_model)],
        )

    item.refresh()
    assert item.my_required_nested_model.sample_field == "foobaz"


def test_transact_update_set_required_deep_model_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    new_deep_nested = MyDeepNestedModel(another_field_bytes_list=[b"hello"])

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[
                Expr(RangeKeyModel).field("my_required_nested_model").field("deep_nested_required").set(new_deep_nested)
            ],
        )

    item.refresh()
    assert item.my_required_nested_model.deep_nested_required.another_field_bytes_list == [b"hello"]


def test_transact_update_set_required_deep_model_field_fails(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with pytest.raises(AttributeInvalidError, match="Attribute: .*doesnt_exist.* not allowed."):
        with TransactWrite() as transaction:
            transaction.update(
                item,
                actions=[Expr(RangeKeyModel).field("my_nested_model").field("doesnt_exist").set({"hello": "world"})],
            )


def test_transact_update_set_dict_field_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[Expr(RangeKeyModel).field("my_required_dict").field("new_field").set("foobaz")],
        )

    item.refresh()
    assert item.my_required_dict["new_field"] == "foobaz"


def test_transact_update_set_optional_dict_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[Expr(RangeKeyModel).field("my_dict").set({"hello": "world"})],
        )

    item.refresh()
    assert item.my_dict == {"hello": "world"}


def test_transact_update_set_dict_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[Expr(RangeKeyModel).field("my_required_dict").set({"foo": "baz"})],
        )

    item.refresh()
    assert item.my_required_dict == {"foo": "baz"}


def test_transact_update_set_tuple_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[Expr(RangeKeyModel).field("my_tuple").set(("hello", 8))],
        )

    item.refresh()
    assert item.my_tuple == ("hello", 8)


def test_transact_update_set_required_nested_model_fails_wrong_model(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    new_model = MyDeepNestedModel(another_field_bytes_list=[b"foobaz"])

    with pytest.raises(AttributeTypeInvalidError, match=f"{new_model.__class__.__name__} input not compatible.*"):
        with TransactWrite() as transaction:
            transaction.update(
                item,
                actions=[Expr(RangeKeyModel).field("my_required_nested_model").set(new_model)],
            )


def test_transact_update_append_optional_deep_nested_model_multiple_actions_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[
                Expr(RangeKeyModel)
                .field("my_nested_model_list")
                .index(1)  # second item has the deep nested model
                .field("deep_nested")
                .index(0)
                .field("another_field_bytes_list")
                .set_append([b"foobar", b"foobaz"]),
                Expr(RangeKeyModel).field("my_str_list").set_append(["z"]),
            ],
        )

    item.refresh()
    assert b"foobar" in item.my_nested_model_list[1].deep_nested[0].another_field_bytes_list
    assert b"foobaz" in item.my_nested_model_list[1].deep_nested[0].another_field_bytes_list
    assert "z" in item.my_str_list
    assert len(item.my_str_list) == 5


def test_transact_update_append_optional_deep_nested_model_fails_idx(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with pytest.raises(AttributeInvalidError, match="Attribute: .*hello.* not allowed."):
        with TransactWrite() as transaction:
            transaction.update(
                item,
                actions=[
                    Expr(RangeKeyModel)
                    .field("my_nested_model_list")
                    .index("hello")  # must be a st or number that parses to int
                    .field("deep_nested")
                    .index(0)
                    .field("another_field_bytes_list")
                    .set_append([b"foobar", b"foobaz"]),
                ],
            )


def test_transact_update_append_required_str_list_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[
                Expr(RangeKeyModel).field("my_required_str_list").index(1).set("hello"),
            ],
        )

    item.refresh()
    assert item.my_required_str_list[1] == "hello"


def test_transact_update_append_required_str_list_wrong_type_fails(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with pytest.raises(AttributeTypeInvalidError, match="integer input not compatible with .*"):
        with TransactWrite() as transaction:
            transaction.update(
                item,
                actions=[
                    Expr(RangeKeyModel).field("my_required_str_list").index(1).set(0),
                ],
            )


def test_transact_update_append_optional_str_list_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[
                Expr(RangeKeyModel).field("my_str_list").index(0).set("hello"),
            ],
        )

    item.refresh()
    assert item.my_str_list[0] == "hello"


def test_transact_update_no_different_actions(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with pytest.raises(UpdateError, match="All actions must be the same. ADD != SET"):
        with TransactWrite() as transaction:
            transaction.update(
                item,
                actions=[
                    Expr(RangeKeyModel).field("my_int").set(9),
                    Expr(RangeKeyModel).field("my_int_set").set_append({9}),
                ],
            )
