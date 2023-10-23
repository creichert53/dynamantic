from typing import Literal, List
from mypy_boto3_dynamodb.type_defs import (
    ProvisionedThroughputTypeDef,
    KeySchemaElementTypeDef,
    ProjectionTableTypeDef,
    ProvisionedThroughputDescriptionTypeDef,
)


class LocalSecondaryIndex:
    key_schema: List[KeySchemaElementTypeDef] = []
    projection: ProjectionTableTypeDef

    hash_key: str
    range_key: str

    def __init__(
        self,
        index_name: str,
        hash_key: str,
        range_key: str | None = None,
        projection: Literal["ALL", "KEYS_ONLY"] = "ALL",
    ):
        self.hash_key = hash_key
        self.range_key = range_key

        # INDEX NAME
        self.index_name = index_name

        # KEY SCHEMA
        self.key_schema = [{"AttributeName": hash_key, "KeyType": "HASH"}]
        if range_key:
            self.key_schema.append({"AttributeName": range_key, "KeyType": "RANGE"})

        # PROJECTION
        self.projection = {"ProjectionType": projection}


class GlobalSecondaryIndex(LocalSecondaryIndex):
    throughput: ProvisionedThroughputDescriptionTypeDef

    def __init__(
        self,
        index_name: str,
        hash_key: str,
        range_key: str | None = None,
        projection: Literal["ALL", "KEYS_ONLY"] = "ALL",
        throughput: ProvisionedThroughputTypeDef | None = None,
    ):
        # PROVISIONED THROUGHPUT
        if throughput is None:
            throughput = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}

        self.throughput = throughput

        super().__init__(index_name, hash_key, range_key, projection)
