import pytest

from dynamantic.batch import BatchGet, BatchWrite
from dynamantic.exceptions import GetError

from tests.conftest import BaseModel, RangeKeyModel, RangeKeyModelTable2, _save_items, _create_item


def test_batch_get_success(dynamodb):
    _save_items(RangeKeyModel)
    _save_items(RangeKeyModelTable2, extra_arg=6)
    with BatchGet() as transaction:
        item1 = transaction.get(RangeKeyModel, "hello:world", "relation_id:hello:world")
        item2 = transaction.get(RangeKeyModelTable2, "foo:bar", "relation_id:foo:bar")

    item2.refresh()
    assert item1.refresh().my_int == 5
    assert item2.refresh().extra_arg == 6


def test_batch_get_success_no_range(dynamodb):
    _save_items(BaseModel)
    with BatchGet() as transaction:
        item1 = transaction.get(BaseModel, "foo:bar")

    assert item1.refresh().my_int == 5


def test_batch_get_fail_dne(dynamodb):
    _save_items(RangeKeyModel)
    with BatchGet() as transaction:
        item1 = transaction.get(RangeKeyModel, "foo:bar", "dne")

    assert item1.refresh() == None


def test_batch_save_success(dynamodb):
    item1 = _create_item(RangeKeyModel, relation_id="range_key")
    item2 = _create_item(RangeKeyModelTable2, relation_id="range_key", extra_arg=6)
    with BatchWrite() as transaction:
        transaction.save(item1)
        transaction.save(item2)

    assert item1.refresh().relation_id == "range_key"
    assert item2.refresh().relation_id == "range_key"
    assert item2.extra_arg == 6


def test_batch_delete_success(dynamodb):
    item1 = _create_item(RangeKeyModel, relation_id="range_key")
    item1.save()
    with BatchWrite() as transaction:
        transaction.delete(item1)

    with pytest.raises(GetError, match="Item doesn't exist.*"):
        assert item1.refresh()


def test_batch_save_and_delete_success(dynamodb):
    first_items = _save_items(BaseModel)
    item1 = _create_item(RangeKeyModel, relation_id="range_key")
    item2 = _create_item(RangeKeyModelTable2, relation_id="range_key", extra_arg=6)

    assert first_items[0].my_int == 5

    with BatchWrite() as transaction:
        transaction.save(item1)
        transaction.save(item2)
        transaction.delete(first_items[0])

    assert item1.refresh().relation_id == "range_key"
    assert item2.refresh().relation_id == "range_key"

    with pytest.raises(GetError, match="Item doesn't exist.*"):
        assert first_items[0].refresh()


def test_batch_save_many(dynamodb):
    all_items = [_create_item(RangeKeyModel, relation_id="range_key") for _ in range(100)]

    with BatchWrite() as transaction:
        for item in all_items:
            transaction.save(item)

    assert all_items[0].refresh().my_int == 5
    assert all_items[99].refresh().my_int == 5


def test_batch_get_many(dynamodb):
    all_items = _save_items(RangeKeyModel, add_count=100)

    futures = []
    with BatchGet() as transaction:
        for item in all_items:
            futures.append(transaction.get(item.__class__, item.item_id, item.relation_id))

    assert futures[0].refresh().my_int == 5
    assert futures[99].refresh().my_int == 96  # 99 - 3 pre-created items
