import json
from typing import Any, Union

from src.patterns.domain_model.mapper import AbstractTranscoder, AbstractTranscoding


class JSONTranscoder(AbstractTranscoder):
    def __init__(self):
        self.types: dict[type, AbstractTranscoding] = {}
        self.names: dict[str, AbstractTranscoding] = {}
        self.encoder = json.JSONEncoder(default=self._encode_dict)
        self.decoder = json.JSONDecoder(object_hook=self._decode_dict)

    def register(self, transcoding: AbstractTranscoding):
        self.types[transcoding.type] = transcoding
        self.names[transcoding.name] = transcoding

    def encode(self, obj: Any) -> bytes:
        return self.encoder.encode(obj).encode('utf8')

    def decode(self, data: bytes) -> Any:
        return self.decoder.decode(data.decode('utf8'))

    def _encode_dict(self, obj: Any) -> dict[str, Union[str, dict]]:
        try:
            transcoding = self.types[type(obj)]
        except KeyError:
            raise TypeError(f'Object of type {obj.__class__.__name__} is not serializable')
        else:
            return {'__type__': transcoding.name, '__data__': transcoding.encode(obj)}

    def _decode_dict(self, data: dict[str, Union[str, dict]]) -> Any:
        if set(data.keys()) == {'__type__', '__data__'}:
            transcoding = self.names[str(data['__type__'])]
            return transcoding.decode(data['__data__'])
        return data
