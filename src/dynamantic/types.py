import typing
import inspect
from typing import Dict, Type, Any

from pydantic import BaseModel


def pydantic_types(cls: BaseModel, key: str, existing: dict = None) -> Dict[str, Any]:
    if existing is None:
        existing = {}

    def traverse(options, full_set: set):
        for option in options:
            if set == typing.get_origin(option):
                full_set.add(set)
            if frozenset == typing.get_origin(option):
                full_set.add(frozenset)
            if list == typing.get_origin(option):
                full_set.add(list)
            if dict == typing.get_origin(option):
                full_set.add(dict)
            if inspect.isclass(option):
                full_set.add(option)
            else:
                traverse(typing.get_args(option), full_set)

    final_set = set()

    type_hint = next(cls.model_fields.get(key_).annotation for key_ in cls.model_fields if key_ == key)

    if inspect.isclass(type_hint):
        final_set.add(type_hint)

    traverse(typing.get_args(type_hint), final_set)

    existing[key] = final_set

    return final_set
