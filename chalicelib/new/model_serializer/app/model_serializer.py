import enum
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Union

from chalicelib.new.config.infra.log import logger as logger
from chalicelib.schema.models import EFOS
from chalicelib.schema.models.tenant import Nomina, Payment

NonObjects = Any
ModelSerialized = dict[str, Any]
FieldStrRepr = str
Token = dict[str, Union["Token", None]]
TokenLeaf = {}

state_mapping = {
    EFOS.StateEnum.DEFINITIVE: "DEFINITIVO",
    EFOS.StateEnum.DISTORTED: "DESVIRTUADO",
    EFOS.StateEnum.ALLEGED: "PRESUNTO",
    EFOS.StateEnum.FAVORABLE_JUDGMENT: "SENTENCIA FAVORABLE",
}

ONE_TO_ONE_RELATIONSHIP_FIELDS = ["nomina"]


@dataclass
class ModelSerializer:
    process_leaf: Callable[[Any], Any] = None
    process_iterable: Callable[[Iterable[Any]], Any] = tuple

    def is_iterable(self, field: Any) -> bool:
        return issubclass(field.__class__, list)

    def is_state_enum(self, field: Any) -> bool:
        return isinstance(field, enum.Enum)

    def _remove_prefix_from_dict_keys(self, data: dict[str, Any], prefix: str) -> dict[str, Any]:
        """Remove a given prefix from all keys in a dictionary."""
        cleaned = {}
        for k, v in data.items():
            new_key = k[len(prefix) :] if k.startswith(prefix) else k
            cleaned[new_key] = v
        return cleaned

    def obj_to_dict(self, record: dict[str, Any], tokens: dict[str, Any]):
        """Create a dictionary based on the token fields"""
        # TODO: Eliminar esta función y usar un serializer genérico
        if record is None:
            return None
        result = {}

        is_dict = isinstance(record, dict)

        # If record is a dict, transform keys with dots into nested dicts
        if is_dict:
            new_record = {}
            for k, v in record.items():
                if "." in k:
                    parts = k.split(".")
                    current = new_record
                    for part in parts[:-1]:
                        if part not in current or not isinstance(current[part], dict):
                            current[part] = {}
                        current = current[part]
                    current[parts[-1]] = v
                else:
                    new_record[k] = v
            record = new_record

        def get_value(record: dict | Any, key: str):
            if key in ONE_TO_ONE_RELATIONSHIP_FIELDS:
                record_as_dict = record._asdict()
                return record_as_dict
            else:
                return record[key] if is_dict else getattr(record, key)

        for key, token in tokens.items():
            real_value = get_value(record, key)
            if isinstance(real_value, enum.Enum):
                if real_value.value in state_mapping:
                    result[key] = state_mapping[real_value.value]
            elif self.is_iterable(real_value):
                first_value = (
                    real_value[0] if len(list(real_value)) > 0 else None
                )  # TODO: Make this more flexible
                if isinstance(first_value, Payment) or key == "payments":
                    result[key] = self.obj_to_dict(first_value, token)
                else:
                    result[key] = (
                        self.process_iterable(self.obj_to_dict(x, token) for x in real_value)
                        or None
                    )
            elif key == "nomina":
                cleaned_real_value = self._remove_prefix_from_dict_keys(real_value, f"{key}.")
                result[key] = self.obj_to_dict(cleaned_real_value, token)

            elif isinstance(real_value, Nomina):
                for field_name in token:
                    result[field_name] = getattr(real_value, field_name)
            elif token == TokenLeaf:
                final_value = self.process_leaf(real_value) if self.process_leaf else real_value
                result[key] = final_value or None
            elif isinstance(real_value, EFOS) and real_value.state.value in state_mapping:
                result[key] = state_mapping[real_value.state.value]
            elif isinstance(real_value, dict):
                result[key] = self.obj_to_dict(real_value, token)
        return result

    def serialize(self, model: Any, fields: Iterable[FieldStrRepr]) -> ModelSerialized:
        """Return a dictionary with the fields given (can use dot to indicate subfields)
        filled with the object data

        Args:
            record (Model): Record to convert
            fields_string (Set[str]): Fields to retrieve

        Returns:
            Dict[str, Any]: Dictionary with the fields filled with the object data
        """
        fields = tuple(fields)
        tokens = tokenize(fields)
        return self.obj_to_dict(model, tokens)


@lru_cache(maxsize=10)
def tokenize(fields: Iterable[FieldStrRepr]) -> Token:
    """Split the fields using the dot character, creating a dict"""
    result: dict[str, Any] = {}
    for field in fields:
        parts = field.split(".")
        current = result
        for part in parts:
            current = current.setdefault(part, dict(TokenLeaf))
    return result
