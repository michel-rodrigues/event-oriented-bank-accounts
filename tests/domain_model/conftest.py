import pytest

from src.domain_model.mapper.transcoders import JSONTranscoder
from src.domain_model.mapper.transcodings import DatetimeAsISO, DecimalAsStr, UUIDAsHex


@pytest.fixture
def json_transcoder():
    transcoder = JSONTranscoder()
    transcoder.register(UUIDAsHex())
    transcoder.register(DecimalAsStr())
    transcoder.register(DatetimeAsISO())
    return transcoder
