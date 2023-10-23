import pytest
from typing import List

from dynamantic import A, K
from dynamantic.exceptions import GetError
from tests.conftest import (
    _save_items,
    BaseModel,
    RangeKeyModel,
    GSI,
    GSIModel,
    LSI,
    LSIModel,
)


def test_get_succeeds(dynamodb):
    _save_items(RangeKeyModel)
    result = RangeKeyModel.get("hello:world", "relation_id:hello:world")
    assert result.my_str == "item1"


def test_get_succeeds_as_dict(dynamodb):
    _save_items(RangeKeyModel)
    result = RangeKeyModel.get("hello:world", "relation_id:hello:world", as_dict=True)
    assert result["my_str"] == "item1"


def test_get_fails(dynamodb):
    _save_items(RangeKeyModel)
    with pytest.raises(GetError, match="Item doesn't exist"):
        RangeKeyModel.get("hello:world", "doesnt_exist")


def test_query_item_by_hash_key(dynamodb):
    _save_items(RangeKeyModel)
    results = RangeKeyModel.query("foo:bar")
    assert len(results) == 1


def test_query_item_by_hash_key_multiple(dynamodb):
    _save_items(RangeKeyModel)
    results = RangeKeyModel.query("hello:world")
    assert len(results) == 2


def test_query_item_by_hash_key_range_condition(dynamodb):
    _save_items(RangeKeyModel)
    results = RangeKeyModel.query(
        "hello:world",
        range_key_condition=K("relation_id").begins_with("relation_id:foo"),
    )
    assert len(results) == 1


def test_query_item_by_hash_key_filter_exists(dynamodb):
    _save_items(RangeKeyModel, add_all=True)
    results: List[RangeKeyModel] = RangeKeyModel.query(
        "hello:world",
        range_key_condition=K("relation_id").begins_with("relation_id:"),
        filter_condition=A("my_int").eq(10),
    )
    assert len(results) == 1
    assert next(iter(results)).my_int == 10


def test_query_item_by_hash_key_filter_not_exists(dynamodb):
    _save_items(RangeKeyModel, add_all=True)
    results = RangeKeyModel.query(
        "hello:world",
        range_key_condition=K("relation_id").begins_with("relation_id:"),
        filter_condition=A("my_int").eq(100),
    )
    assert len(results) == 0


def test_query_with_attributes(dynamodb):
    _save_items(RangeKeyModel, add_all=True)
    results = RangeKeyModel.query(
        "hello:world",
        range_key_condition=K("relation_id").begins_with("relation_id:"),
        filter_condition=A("my_int").eq(10),
        attributes_to_get=["my_int", "my_str"],
        as_dict=True,
    )
    item: RangeKeyModel = next(iter(results))
    assert item["my_int"] == 10
    assert "item_id" in item
    assert "relation_id" in item
    assert "my_str" in item
    assert "my_bool" not in item


def test_query_with_attributes_no_range(dynamodb):
    _save_items(BaseModel)
    results = BaseModel.query(
        "hello:world",
        as_dict=True,
    )
    item: BaseModel = next(iter(results))
    assert item["my_int"] == 5


def test_query_gsindex_single(dynamodb):
    _save_items(GSIModel)
    results = GSIModel.query(
        "item1",
        index=GSI,
        range_key_condition=K("relation_id").begins_with("relation_id:"),
    )
    assert len(results) == 1


def test_query_gsindex_multiple(dynamodb):
    _save_items(GSIModel)
    results = GSIModel.query(
        "item2",
        index=GSI,
        range_key_condition=K("relation_id").begins_with("relation_id:foo"),
    )
    assert len(results) == 2


def test_query_lsindex_single(dynamodb):
    _save_items(LSIModel)
    results = LSIModel.query(
        "item1",
        index=LSI,
        range_key_condition=K("relation_id").begins_with("relation_id:"),
    )
    assert len(results) == 1