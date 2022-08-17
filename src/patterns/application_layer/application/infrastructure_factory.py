import abc
import os
from distutils.util import strtobool

from src.patterns.domain_model_layer.aggregate import resolve_topic
from src.patterns.domain_model_layer.event_store import EventStore
from src.patterns.domain_model_layer.mapper import AbstractTranscoder, Mapper
from src.patterns.domain_model_layer.notification import ApplicationRecorder
from src.patterns.domain_model_layer.recorder import AggregateRecorder


class InfrastructureFactory(abc.ABC):
    TOPIC = 'INFRASTRUCTURE_FACTORY_TOPIC'
    CIPHER_TOPIC = 'CIPHER_TOPIC'
    CIPHER_KEY = 'CIPHER_KEY'
    MAPPER_TOPIC = 'MAPPER_TOPIC'
    COMPRESSOR_TOPIC = 'COMPRESSOR_TOPIC'
    IS_SNAPSHOTTING_ENABLED = 'IS_SNAPSHOTTING_ENABLED'

    @classmethod
    def construct(cls, name) -> 'InfrastructureFactory':
        topic = os.getenv(cls.TOPIC, 'src.application.infrastructure.in_memory#InMemoryInfrastructureFactory')
        try:
            factory_cls = resolve_topic(topic)
        except (ModuleNotFoundError, AttributeError):
            raise EnvironmentError(
                'Failed to resolve '
                'infrastructure factory topic: '
                f"'{topic}' from environment "
                f"variable '{cls.TOPIC}'"
            )
        if not issubclass(factory_cls, InfrastructureFactory):
            raise AssertionError(f'Not an infrastructure factory: {topic}')
        return factory_cls(application_name=name)

    def __init__(self, application_name):
        self.application_name = application_name

    def getenv(self, key, default=None, application_name=''):
        if not application_name:
            application_name = self.application_name
        keys = [
            application_name.upper() + '_' + key,
            key,
        ]
        for key in keys:
            value = os.getenv(key)
            if value:
                return value
        return default

    def mapper(self, transcoder: AbstractTranscoder, application_name: str = '') -> Mapper:
        cipher_topic = self.getenv(self.CIPHER_TOPIC, application_name=application_name)
        cipher_key = self.getenv(self.CIPHER_KEY, application_name=application_name)
        cipher = None
        compressor = None
        if cipher_topic:
            if cipher_key:
                cipher_cls = resolve_topic(cipher_topic)
                cipher = cipher_cls(cipher_key=cipher_key.encode('utf8'))
            else:
                raise EnvironmentError('Cipher key was not found in env, although cipher topic was found')
        compressor_topic = self.getenv(self.COMPRESSOR_TOPIC)
        if compressor_topic:
            compressor = resolve_topic(compressor_topic)
        return Mapper(transcoder=transcoder, cipher=cipher, compressor=compressor)

    def event_store(self, **kwargs) -> EventStore:
        return EventStore(**kwargs)

    @abc.abstractmethod
    def aggregate_recorder(self) -> AggregateRecorder:
        pass

    @abc.abstractmethod
    def application_recorder(self) -> ApplicationRecorder:
        pass

    # @abc.abstractmethod
    # def process_recorder(self) -> ProcessRecorder:
    #     pass

    def is_snapshotting_enabled(self) -> bool:
        default = 'no'
        return bool(strtobool(self.getenv(self.IS_SNAPSHOTTING_ENABLED, default) or default))
