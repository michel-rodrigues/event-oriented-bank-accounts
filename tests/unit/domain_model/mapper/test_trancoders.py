import uuid
from datetime import datetime
from decimal import Decimal

from src.domain_model.mapper.transcoders import JSONTranscoder


def test_transcode_a_dict_of_objects_with_registered_trancodings(json_transcoder: JSONTranscoder):
    dict_objects = {
        'not_important_key_name_1': uuid.uuid4(),
        'not_important_key_name_2': datetime.now(),
        'not_important_key_name_3': Decimal('12.34'),
    }

    data = json_transcoder.encode(dict_objects)
    assert isinstance(data, bytes)

    copy = json_transcoder.decode(data)
    assert copy == dict_objects
