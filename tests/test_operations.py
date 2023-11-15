import pytest

from dynamantic.main import Expr
from dynamantic.attrs import A
from dynamantic.exceptions import PutError, UpdateError, GetError, DeleteError

from tests.conftest import BaseModel, RangeKeyModel, _create_item


def test_new_item(dynamodb):
    item = _create_item(BaseModel)
    item.save(condition_expression=A("my_str").not_exists() & A("my_int").not_exists())
    item.my_int = 6
    item.save()
    assert item.my_int == 6


def test_new_item_float_succeeds(dynamodb):
    item = _create_item(BaseModel)
    item.my_float = 0.01
    item.save()
    assert item.my_float == 0.01


def test_new_item_fails(dynamodb):
    item = _create_item(BaseModel)
    original_int = item.my_int
    item.save()
    item.my_int = 10
    with pytest.raises(PutError) as excinfo:
        item.save(condition_expression=A("my_int").lt(3))
    assert item.my_int == original_int  # unchanged


def test_update_item_hash_only(dynamodb):
    item = _create_item(BaseModel)
    item.save()
    item.update(actions=[Expr(BaseModel).field("my_required_str_set").set_append({"hello"})])
    assert "hello" in item.my_required_str_set


def test_update_item_with_range(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()
    item.update(actions=[Expr(RangeKeyModel).field("my_required_str_set").set_append({"hello"})])
    assert "hello" in item.my_required_str_set


def test_update_item_fails(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    original_int = item.my_int
    item.save()
    item.my_int = 10
    with pytest.raises(UpdateError) as excinfo:
        item.update(
            actions=[Expr(RangeKeyModel).field("my_required_str_set").set_append({"hello"})],
            condition_expression=A("my_int").lt(3),
        )
    assert item.my_int == original_int  # unchanged


def test_delete_item(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    item.save()
    item.delete()
    with pytest.raises(GetError):
        item.refresh()  # confirm item is deleted


def test_delete_item_fails(dynamodb):
    item = _create_item(RangeKeyModel, relation_id="range_key")
    with pytest.raises(DeleteError):
        item.delete(condition_expression=A("doesnt_exist").lt(3))
