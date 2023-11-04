import pytest

from dynamantic.main import _DynamanticFuture
from dynamantic.transactions import TransactGet, TransactWrite
from dynamantic.exceptions import TransactGetError
from dynamantic.expressions import Expr

from tests.conftest import BaseModel, RangeKeyModel, MyNestedModel, _save_items, _create_item


# def test_transact_get_success(dynamodb):
#     f = _DynamanticFuture(model_cls=RangeKeyModel)
#     f.refresh()

#     _save_items(RangeKeyModel)
#     with TransactGet() as transaction:
#         item1 = transaction.get(RangeKeyModel, "hello:world", "relation_id:hello:world")
#         item2 = transaction.get(RangeKeyModel, "foo:bar", "relation_id:foo:bar")

#     assert item1.refresh().my_int == 5
#     assert item2.model_dump()["my_int"] == 5


# def test_transact_get_success_no_range(dynamodb):
#     _save_items(BaseModel)
#     with TransactGet() as transaction:
#         item1 = transaction.get(BaseModel, "foo:bar")

#     assert item1.refresh().my_int == 5


# def test_transact_get_fail_dne(dynamodb):
#     _save_items(RangeKeyModel)
#     with pytest.raises(TransactGetError, match=".* does not exist"):
#         with TransactGet() as transaction:
#             transaction.get(RangeKeyModel, "foo:bar", "dne")


# def test_transact_save_success(dynamodb):
#     item = _create_item(RangeKeyModel, relation_id="range_key")
#     with TransactWrite() as transaction:
#         transaction.save(item)

#     assert item.refresh().my_int == 5


# def test_transact_delete_success(dynamodb):
#     item = _create_item(RangeKeyModel, relation_id="range_key")
#     item.save()

#     with TransactWrite() as transaction:
#         transaction.delete(item)

#     assert len(RangeKeyModel.scan()) == 0


def test_transact_udpate_set_single_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    original_my_int = item.my_int
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_int").set(9)])

    item.refresh()
    assert item.my_int == 9
    assert item.my_int != original_my_int


def test_transact_udpate_set_multiple_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    original_my_int = item.my_int
    original_my_bool = item.my_bool
    item.save()
    print([vars(field) for field in Expr(RangeKeyModel).field("my_int").set(9)._expr._fields])

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


def test_transact_udpate_set_add_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    original_my_int = item.my_int
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_int").set_add(9)])

    assert item.refresh().my_int == original_my_int + 9


def test_transact_udpate_set_subtract_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    original_my_float = item.my_float
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_float").set_add(-9.2)])

    assert item.refresh().my_float == pytest.approx(original_my_float - 9.2, rel=0.002)


def test_transact_udpate_set_int_list_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_int_list").set_append([9])])

    item.refresh()
    assert 9 in item.my_int_list
    assert len(item.my_int_list) == 4


def test_transact_udpate_set_int_set_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_int_set").set_append({9})])

    item.refresh()
    assert 9 in item.my_int_set
    assert len(item.my_int_set) == 3


def test_transact_udpate_set_bytes_set_fails(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    # Cannot append bytes to bytes set
    with pytest.raises(Exception):
        with TransactWrite() as transaction:
            transaction.update(item, actions=[Expr(RangeKeyModel).field("my_bytes_set").set_append({b"zebra"})])


def test_transact_udpate_set_bytes_list_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_bytes_list").set_append([b"zebra"])])

    item.refresh()
    assert b"zebra" in item.my_bytes_list
    assert len(item.my_bytes_list) == 4


def test_transact_udpate_set_string_set_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    with TransactWrite() as transaction:
        transaction.update(item, actions=[Expr(RangeKeyModel).field("my_str_set").set_append({"z"})])

    item.refresh()
    assert "z" in item.my_str_set
    assert len(item.my_str_set) == 4


def test_transact_udpate_append_nested_model_list_succeeds(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()

    new_model = MyNestedModel(sample_field="foobaz")

    with TransactWrite() as transaction:
        transaction.update(
            item,
            actions=[Expr(RangeKeyModel).field("my_nested_model_list").set_append([new_model])],
        )

    item.refresh()
    assert item.my_nested_model_list[-1].sample_field == "foobaz"
