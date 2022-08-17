import abc
import uuid
from typing import Optional

from src.application.notification_log import LocalNotificationLog
from src.domain_model.mapper.transcoders import JSONTranscoder
from src.domain_model.mapper.transcodings import DatetimeAsISO, DecimalAsStr, UUIDAsHex
from src.patterns.application_layer.application.infrastructure_factory import InfrastructureFactory
from src.patterns.application_layer.repository import Repository
from src.patterns.application_layer.snapshot import Snapshot
from src.patterns.domain_model_layer.aggregate import Aggregate
from src.patterns.domain_model_layer.event_store import EventStore
from src.patterns.domain_model_layer.mapper import AbstractTranscoder, Mapper
from src.patterns.domain_model_layer.notification import ApplicationRecorder


class Application(abc.ABC):
    def __init__(self):
        self.factory = self.construct_factory()
        self.mapper = self.construct_mapper()
        self.recorder = self.construct_recorder()
        self.events = self.construct_event_store()
        self.snapshots = self.construct_snapshot_store()
        self.repository = self.construct_repository()
        self.log = self.construct_notification_log()

    def construct_factory(self) -> InfrastructureFactory:
        return InfrastructureFactory.construct(self.__class__.__name__)

    def construct_mapper(self, application_name='') -> Mapper:
        return self.factory.mapper(
            transcoder=self.construct_transcoder(),
            application_name=application_name,
        )

    def construct_transcoder(self) -> AbstractTranscoder:
        transcoder = JSONTranscoder()
        self.register_transcodings(transcoder)
        return transcoder

    def register_transcodings(self, transcoder: AbstractTranscoder):
        transcoder.register(UUIDAsHex())
        transcoder.register(DecimalAsStr())
        transcoder.register(DatetimeAsISO())

    def construct_recorder(self) -> ApplicationRecorder:
        return self.factory.application_recorder()

    def construct_event_store(
        self,
    ) -> EventStore:
        return self.factory.event_store(mapper=self.mapper, recorder=self.recorder)

    def construct_snapshot_store(self) -> Optional[EventStore]:
        if not self.factory.is_snapshotting_enabled():
            return None

        recorder = self.factory.aggregate_recorder()
        return self.factory.event_store(mapper=self.mapper, recorder=recorder)

    def construct_repository(self):
        return Repository(event_store=self.events, snapshot_store=self.snapshots)

    def construct_notification_log(self):
        return LocalNotificationLog(self.recorder, section_size=10)

    def save(self, *aggregates: Aggregate) -> None:
        events = []
        for aggregate in aggregates:
            events += aggregate._collect_()
            self.events.put(events)
            self.notify(events)

    def notify(self, new_events: list[Aggregate.Event]):
        pass

    def take_snapshot(self, aggregate_id: uuid.UUID, version: int):
        aggregate = self.repository.get(aggregate_id, version)
        snapshot = Snapshot.take(aggregate)
        self.snapshots.put([snapshot])
