# pylint: disable=W0212

from typing import List, Any, Type, Dict, Tuple

from mypy_boto3_dynamodb.type_defs import BatchGetItemInputRequestTypeDef, BatchWriteItemInputRequestTypeDef

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

from dynamantic.main import Dynamantic, T, _DynamanticFuture


class BatchContext:
    _operations: List[Tuple[str, Dict]] = []
    _futures: List[Tuple[str, _DynamanticFuture]] = []
    _models: List[Tuple[str, T]] = {}

    def __init__(self) -> None:
        self._operations = []
        self._futures = []
        self._models = []

    def __enter__(self):
        return self

    def _add_model(self, model: T) -> _DynamanticFuture[T]:
        model_future = _DynamanticFuture(model_cls=model)
        tn = model.__table_name__

        self._futures.append((tn, model_future))
        self._models.append((tn, model))

        return model_future


class BatchWrite(BatchContext):
    _operations: List[Tuple[str, BatchWriteItemInputRequestTypeDef]] = []

    def _primary_key(self, item: T) -> Dict[str, Dict]:
        update_item = {k: TypeSerializer().serialize(v) for k, v in item.serialize().items()}
        primary_key = {}
        for k, v in update_item.items():
            if k in (item.__hash_key__, item.__range_key__):
                primary_key[k] = v
        return primary_key

    def save(self, item: T) -> None:
        put_item = {k: TypeSerializer().serialize(v) for k, v in item.serialize().items()}
        self._operations.append((item.__table_name__, {"PutRequest": {"Item": put_item}}))
        self._add_model(item.__class__)

    def delete(self, item: T) -> None:
        self._operations.append((item.__table_name__, {"DeleteRequest": {"Key": self._primary_key(item)}}))
        self._add_model(item.__class__)

    def __exit__(self, exc_type, exc_value, traceback):
        # Requests are chunked into 25 at a time
        model: Dynamantic = next(iter(self._models))[1]
        chunked = [self._operations[i : i + 25] for i in range(0, len(self._operations), 25)]
        for chunk in chunked:
            request = {}
            for op in chunk:
                if op[0] not in request:
                    request[op[0]] = []
                request[op[0]].append(op[1])
            model._dynamodb().batch_write_item(RequestItems=request)


class BatchGet(BatchContext):
    _operations: List[Tuple[str, BatchGetItemInputRequestTypeDef]] = []

    def get(self, model: Type[T], hash_key: Any, range_key: Any = None) -> _DynamanticFuture[T]:
        key = model._key(hash_key, range_key)
        self._operations.append((model.__table_name__, key))
        return self._add_model(model)

    def __exit__(self, exc_type, exc_value, traceback):
        # Requests are chunked into 25 at a time
        model: Dynamantic = next(iter(self._models))[1]
        chunked = [self._operations[i : i + 25] for i in range(0, len(self._operations), 25)]
        for i, chunk in enumerate(chunked):
            request = {}
            for op in chunk:
                if op[0] not in request:
                    request[op[0]] = {"Keys": []}
                request[op[0]]["Keys"].append(op[1])
            results = model._dynamodb().batch_get_item(RequestItems=request)["Responses"]

            counter = 0
            for _, items in results.items():
                for item in items:
                    item = {k: TypeDeserializer().deserialize(v) for k, v in item.items()}
                    self._futures[25 * i + counter][1].from_raw_data(item)
                    counter = counter + 1
