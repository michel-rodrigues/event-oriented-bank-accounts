import uuid
from datetime import datetime
from decimal import Decimal
from typing import Union

from src.patterns.domain_model_layer.mapper import AbstractTranscoding


class UUIDAsHex(AbstractTranscoding):
    type = uuid.UUID
    name = 'uuid_hex'

    def encode(self, obj: uuid.UUID) -> str:
        return obj.hex

    def decode(self, data: Union[str, dict]) -> uuid.UUID:
        assert isinstance(data, str)
        return uuid.UUID(data)


class DecimalAsStr(AbstractTranscoding):
    type = Decimal
    name = 'decimal_str'

    def encode(self, obj: Decimal):
        return str(obj)

    def decode(self, data: Union[str, dict]) -> Decimal:
        assert isinstance(data, str)
        return Decimal(data)


class DatetimeAsISO(AbstractTranscoding):
    type = datetime
    name = 'datetime_iso'

    def encode(self, obj: datetime) -> str:
        return obj.isoformat()

    def decode(self, data: Union[str, dict]) -> datetime:
        assert isinstance(data, str)
        return datetime.fromisoformat(data)
