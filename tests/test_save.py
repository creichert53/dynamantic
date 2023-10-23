import pytest

from dynamantic import A
from dynamantic.exceptions import PutError

from tests.conftest import _create_item, BaseModel


def test_new_item(dynamodb):
    item: BaseModel = _create_item(BaseModel)
    item.save(condition_expression=A("my_str").not_exists() & A("my_int").not_exists())
    item.my_int = 6
    item.save()
    assert item.my_int == 6


def test_new_item_fails(dynamodb):
    item: BaseModel = _create_item(BaseModel)
    original_int = item.my_int
    item.save()  # save first, then change, then refresh
    item.my_int = 10
    with pytest.raises(PutError) as excinfo:
        item.save(condition_expression=A("my_int").lt(3))
    assert item.my_int == original_int  # unchanged
