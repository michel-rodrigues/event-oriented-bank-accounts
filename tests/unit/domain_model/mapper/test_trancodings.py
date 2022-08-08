import uuid
from datetime import datetime
from decimal import Decimal

from src.domain_model.mapper.transcodings import DatetimeAsISO, DecimalAsStr, UUIDAsHex


def test_encode_uuid_as_hex():
    transcoding = UUIDAsHex()
    uuid_object = uuid.uuid4()
    assert transcoding.encode(uuid_object) == uuid_object.hex


def test_decode_hex_as_uuid():
    transcoding = UUIDAsHex()
    uuid_object = uuid.uuid4()
    assert transcoding.decode(uuid_object.hex) == uuid_object


def test_encode_datetime_as_isoformat():
    transcoding = DatetimeAsISO()
    datetime_object = datetime.now()
    assert transcoding.encode(datetime_object) == datetime_object.isoformat()


def test_decode_isoformat_as_datetime():
    transcoding = DatetimeAsISO()
    datetime_object = datetime.now()
    assert transcoding.decode(datetime_object.isoformat()) == datetime_object


def test_encode_decimal_as_str():
    transcoding = DecimalAsStr()
    decimal_object = Decimal('12.34')
    assert transcoding.encode(decimal_object) == str(decimal_object)


def test_decode_str_as_decimal():
    transcoding = DecimalAsStr()
    decimal_object = Decimal('12.34')
    assert transcoding.decode(str(decimal_object)) == decimal_object
