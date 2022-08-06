import abc
import uuid
from copy import deepcopy
from typing import Any, Union

from src.patterns.domain_model import DomainEvent, ImmutableObject
from src.patterns.domain_model.aggregate import get_topic, resolve_topic


class AbstractTranscoder(abc.ABC):
    @abc.abstractmethod
    def encode(self, obj: Any) -> bytes:
        pass

    @abc.abstractmethod
    def decode(self, data: bytes) -> Any:
        pass


class AbstractTranscoding(abc.ABC):
    @property
    @abc.abstractmethod
    def type(self) -> type:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @abc.abstractmethod
    def encode(self, obj: Any) -> Union[str, dict]:
        pass

    @abc.abstractmethod
    def decode(self, data: Union[str, dict]) -> Any:
        pass


class AbstractCompressor(abc.ABC):
    @abc.abstractmethod
    def compress(self, data: bytes) -> bytes:
        pass

    @abc.abstractmethod
    def decompress(self, data: bytes) -> bytes:
        pass


class AbstractCipher(abc.ABC):
    @abc.abstractmethod
    def encrypt(self, data: bytes) -> bytes:
        pass

    @abc.abstractmethod
    def decrypt(self, data: bytes) -> bytes:
        pass


class StoredEvent(ImmutableObject):
    originator_id: uuid.UUID
    originator_version: int
    topic: str
    state: bytes


class Mapper:
    def __init__(
        self,
        transcoder: AbstractTranscoder,
        compressor: AbstractCompressor = None,
        cipher: AbstractCipher = None,
    ):
        self._transcoder = transcoder
        self._compressor = compressor
        self._cipher = cipher

    def from_domain_event(self, domain_event: DomainEvent) -> StoredEvent:
        topic: str = get_topic(domain_event.__class__)
        domain_event_data = deepcopy(vars(domain_event))
        domain_event_data.pop('originator_id')
        domain_event_data.pop('originator_version')
        state: bytes = self._transcoder.encode(domain_event_data)
        if self._compressor:
            state = self._compressor.compress(state)
        if self._cipher:
            state = self._cipher.encrypt(state)
        return StoredEvent(
            originator_id=domain_event.originator_id,
            originator_version=domain_event.originator_version,
            topic=topic,
            state=state,
        )

    def to_domain_event(self, stored: StoredEvent) -> DomainEvent:
        state: bytes = stored.state
        if self._cipher:
            state = self._cipher.decrypt(state)
        if self._compressor:
            state = self._compressor.decompress(state)
        domain_event_data = self._transcoder.decode(state)
        domain_event_data['originator_id'] = stored.originator_id
        domain_event_data['originator_version'] = stored.originator_version
        cls = resolve_topic(stored.topic)
        assert issubclass(cls, DomainEvent)
        domain_event: DomainEvent = object.__new__(cls)
        vars(domain_event).update(domain_event_data)
        return domain_event
