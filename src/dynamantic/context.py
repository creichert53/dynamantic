# pylint: disable=W0212

from typing import List, Any, Type, Dict

from boto3.dynamodb.types import TypeSerializer

from dynamantic.main import T, ConditionExpression, _DynamanticFuture


class Context:
    _operations: List = []
    _futures: List[_DynamanticFuture] = []
    _models: List[T] = []

    def __init__(self) -> None:
        self._operations = []
        self._futures: List[_DynamanticFuture] = []
        self._models: List[T] = []

    def __enter__(self):
        return self

    def _add_model(self, model: T) -> _DynamanticFuture[T]:
        model_future = _DynamanticFuture(model_cls=model)
        self._futures.append(model_future)
        self._models.append(model)
        return model_future


class ContextWrite(Context):
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
        self._operations.append({"Put": {"Item": put_item, "TableName": item.__table_name__}})
        self._add_model(item.__class__)

    def update(self, item: T, actions: List[ConditionExpression]) -> Dict:
        """Perform a transact UPDATE operation on the database."""
        last_action_type, all_operations, all_attribute_values = item._update(actions)

        self._operations.append(
            {
                "Update": {
                    "Key": self._primary_key(item),
                    "TableName": item.__table_name__,
                    "UpdateExpression": " ".join([last_action_type, ", ".join(all_operations)]),
                    "ExpressionAttributeValues": all_attribute_values,
                }
            }
        )
        self._add_model(item.__class__)

    def delete(self, item: T) -> None:
        """Perform a transact DELETE operation on the database."""
        self._operations.append({"Delete": {"Key": self._primary_key(item), "TableName": item.__table_name__}})
        self._add_model(item.__class__)

    def __exit__(self, exc_type, exc_value, traceback):
        # Placeholder
        pass


class ContextGet(Context):
    def _key(self, model: Type[T], hash_key: Any, range_key: Any = None) -> Dict[str, Any]:
        key = {model.__hash_key__: {model._dynamodb_type(model.__hash_key__): hash_key}}
        if range_key:
            key[model.__range_key__] = {model._dynamodb_type(model.__range_key__): range_key}
        return key

    def get(self, model: Type[T], hash_key: Any, range_key: Any = None):
        # Placeholder
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        # Placeholder
        pass
