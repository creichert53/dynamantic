import pytest

from dynamantic.exceptions import InvalidStateError
from dynamantic import A, K
from tests.conftest import (
    _save_items,
    BaseModel,
    RangeKeyModel,
    GSI,
    GSIModel,
    LSI,
    LSIModel,
)


def test_scan_item_no_range(dynamodb):
    _save_items(BaseModel)
    # without a range key, only one "hello:world" item will exist
    results = BaseModel.scan(filter_condition=A("item_id").eq("hello:world"), as_dict=True)
    assert len(results) == 1


def test_scan_item_with_range(dynamodb):
    _save_items(RangeKeyModel)
    # with a range key, "hello:world" item will exist in 2 items
    results = RangeKeyModel.scan(filter_condition=A("item_id").eq("hello:world"), as_dict=True)
    assert len(results) == 2


def test_scan_gsi(dynamodb):
    _save_items(GSIModel)
    results = GSIModel.scan(index=GSI)
    assert len(results) == 3


def test_scan_gsi_with_filter_succeeds(dynamodb):
    _save_items(GSIModel, add_all=True)
    results = GSIModel.scan(index=GSI, filter_condition=A("my_int").gte(47))
    assert len(results) == 3


def test_scan_gsi_with_filter_raises_invalid_state(dynamodb):
    _save_items(RangeKeyModel, add_all=True)
    with pytest.raises(InvalidStateError, match="Index provided but index does not exist for model."):
        RangeKeyModel.scan(index=GSI, filter_condition=A("my_int").gte(47))
