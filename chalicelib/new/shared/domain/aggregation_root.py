import enum
import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, TypeVar, Union, get_origin
from uuid import UUID

from chalicelib.new.shared.domain.primitives import Identifier, identifier_default_factory

JSON_SERIALIZERS = {
    datetime: lambda x: x.isoformat(),
    date: lambda x: x.isoformat(),
    UUID: str,
    enum.Enum: lambda x: x.value,
}

INVERSE_JSON_SERIALIZERS = {
    datetime: lambda x: datetime.fromisoformat(x),
    date: lambda x: date.fromisoformat(x),
    UUID: UUID,
    enum.Enum: lambda x: x,
    int: lambda x: int(x or 0),
    tuple: lambda x: tuple(x or []),
    list: lambda x: list(x or []),
}


def get_type(obj: Any):
    return enum.Enum if isinstance(obj, enum.Enum) else type(obj)


def json_serial(obj):
    serializer = JSON_SERIALIZERS.get(get_type(obj))
    if not serializer:
        raise TypeError(f"Type {type(obj)} not serializable")
    return serializer(obj)


TypeAggregationRoot = TypeVar("TypeAggregationRoot", bound="AggregationRoot")


@dataclass
class AggregationRoot:
    identifier: Identifier = field(default_factory=identifier_default_factory, init=False)
    execute_at: datetime | None = field(default=None, init=False)

    orm_blacklist = {
        "orm_blacklist",
        "execute_at",
    }

    def get_delay(self, max_delay: timedelta | None = None) -> timedelta | None:
        """Get the delay until the message is executed."""
        if not self.execute_at:
            return None
        desired = self.execute_at - datetime.utcnow()
        no_delay = timedelta(0)
        desired = max(no_delay, desired)
        return min(desired, max_delay) if max_delay else desired

    def json(self) -> str:
        return json.dumps(self.to_dict(), default=json_serial)

    def set_identifier(self, identifier: Identifier) -> TypeAggregationRoot:
        self.identifier = identifier or identifier_default_factory()
        return self

    @classmethod
    def from_json(cls, json_str: str) -> TypeAggregationRoot:
        dict_repr: dict[str, Any] = json.loads(json_str)
        identifier = dict_repr.pop("identifier", None)
        model = cls(**dict_repr)
        if identifier:
            model.set_identifier(identifier)
        model.sanitize()
        return model

    def sanitize(self):
        """Cast to correct types"""
        for field_name, field_type in self.__annotations__.items():
            value = getattr(self, field_name)
            # In case of union, use the first type
            origin_type = get_origin(field_type)
            if origin_type is Union:
                origin_type = field_type.__args__[0]
            real_type = (
                field_type if issubclass(origin_type or field_type, enum.Enum) else origin_type
            )
            if real_type is None:
                real_type = field_type

            if value is None:
                continue
            if real_type is not type(value):
                serializer = INVERSE_JSON_SERIALIZERS.get(real_type, real_type)
                try:
                    serializer(value)
                    setattr(self, field_name, serializer(value))
                except:
                    raise

    def to_dict(self):
        return asdict(self)

    def to_orm_dict(self):
        orm_dict = self.to_dict()
        for key in self.orm_blacklist:
            orm_dict.pop(key, None)
        return orm_dict
