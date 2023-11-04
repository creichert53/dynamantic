# pylint: disable=W0212

from typing import List, Any, Type, Dict

from mypy_boto3_dynamodb.type_defs import TransactGetItemTypeDef, TransactWriteItemTypeDef

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

from dynamantic.exceptions import TransactGetError, UpdateError
from dynamantic.main import T, Dynamantic, _DynamanticFuture
from dynamantic.expressions import ConditionExpression


class Transact:
    _transactions: List[TransactGetItemTypeDef] = []
    _futures: List[_DynamanticFuture] = []
    _models: List[T] = []

    def __init__(self) -> None:
        self._transactions = []
        self._futures: List[_DynamanticFuture] = []
        self._models: List[T] = []

    def __enter__(self):
        return self

    def _add_model(self, model: T) -> _DynamanticFuture[T]:
        model_future = _DynamanticFuture(model_cls=model)
        self._futures.append(model_future)
        self._models.append(model)
        return model_future


class TransactWrite(Transact):
    _transactions: List[TransactWriteItemTypeDef] = []

    def _primary_key(self, item: T) -> Dict[str, Dict]:
        update_item = {k: TypeSerializer().serialize(v) for k, v in item.serialize().items()}
        primary_key = {}
        for k, v in update_item.items():
            if k in (item.__hash_key__, item.__range_key__):
                primary_key[k] = v
        return primary_key

    def save(self, item: T) -> None:
        """Perform a transact PUT operation on the database."""
        put_item = {k: TypeSerializer().serialize(v) for k, v in item.serialize().items()}
        self._transactions.append({"Put": {"Item": put_item, "TableName": item.__table_name__}})
        self._add_model(item.__class__)

    def update(self, item: T, actions: List[ConditionExpression]) -> Dict:
        """Perform a transact UPDATE operation on the database."""
        last_action_type = None
        all_actions = []
        all_attribute_values = {}
        for action in actions:
            action_parts = action.update_expression.split(" ")
            action_type = action_parts.pop(0)
            if last_action_type is None:
                last_action_type = action_type
            elif action_type != last_action_type:
                raise UpdateError(f"All actions must be the same. {action_type} != {last_action_type}")
            all_actions.append(" ".join(action_parts))
            all_attribute_values = all_attribute_values | action.expression_attribute_values

        self._transactions.append(
            {
                "Update": {
                    "Key": self._primary_key(item),
                    "TableName": item.__table_name__,
                    "UpdateExpression": " ".join([last_action_type, ", ".join(all_actions)]),
                    "ExpressionAttributeValues": all_attribute_values,
                }
            }
        )
        self._add_model(item.__class__)

    def delete(self, item: T) -> None:
        """Perform a transact DELETE operation on the database."""
        self._transactions.append({"Delete": {"Key": self._primary_key(item), "TableName": item.__table_name__}})
        self._add_model(item.__class__)

    def __exit__(self, exc_type, exc_value, traceback):
        if len(self._transactions) > 0:
            # need to get single instance of the boto3 client
            model: Dynamantic = next(iter(self._models))
            print(self._transactions)
            model._dynamodb().transact_write_items(TransactItems=self._transactions)


class TransactGet(Transact):
    def get(self, model: Type[T], hash_key: Any, range_key: Any = None) -> _DynamanticFuture[T]:
        """Perform a transact GET operation on the database.

        Args:
            model (Type[T]): The Dynamantic Subclass of the item to get.
            hash_key (Any): Table Hash Key value to search for.
            range_key (Any, optional): If table has a Range (sort) Key, the value you are searching for
                                        must be provided here. Defaults to None.

        Returns:
            _DynamanticFuture[T]: A placeholder object that will be used to reproduce the future result
                                        of the get operation.
        """
        key = {model.__hash_key__: {model._dynamodb_type(model.__hash_key__): hash_key}}
        if range_key:
            key[model.__range_key__] = {model._dynamodb_type(model.__range_key__): range_key}
        self._transactions.append({"Get": {"TableName": model.__table_name__, "Key": key}})
        return self._add_model(model)

    def __exit__(self, exc_type, exc_value, traceback):
        if len(self._transactions) > 0:
            # need to get single instance of the boto3 client
            model: Dynamantic = next(iter(self._models))

            items = model._dynamodb().transact_get_items(TransactItems=self._transactions)["Responses"]
            for x, item in enumerate(items):
                if "Item" not in item:
                    for _, v in self._transactions[x].items():
                        key = v["Key"]
                        raise TransactGetError(f"{key} does not exist in the table.")
                item = {k: TypeDeserializer().deserialize(v) for k, v in item["Item"].items()}
                self._futures[x].from_raw_data(item)
