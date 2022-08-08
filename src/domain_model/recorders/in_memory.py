import uuid
from collections import defaultdict
from threading import Lock

from src.patterns.domain_model.mapper import StoredEvent
from src.patterns.domain_model.recorder import AggregateRecorder


class InMemoryAggregateRecorder(AggregateRecorder):
    def __init__(self):
        self.stored_events: list[StoredEvent] = []
        self.stored_events_index: dict[uuid.UUID, dict[int, int]] = defaultdict(dict)
        self.database_lock = Lock()

    def insert_events(self, stored_events: list[StoredEvent], **kwargs) -> None:
        with self.database_lock:
            self.assert_uniqueness(stored_events, **kwargs)
            self.update_table(stored_events, **kwargs)

    def assert_uniqueness(self, stored_events: list[StoredEvent], **kwargs) -> None:
        for stored_event in stored_events:
            if stored_event.originator_version in self.stored_events_index[stored_event.originator_id]:
                raise self.IntegrityError

    def update_table(self, stored_events: list[StoredEvent], **kwargs) -> None:
        for stored_event in stored_events:
            self.stored_events.append(stored_event)
            self.stored_events_index[stored_event.originator_id][stored_event.originator_version] = (
                len(self.stored_events) - 1
            )

    def select_events(
        self,
        originator_id: uuid.UUID,
        gt: int = None,
        lte: int = None,
        desc: bool = False,
        limit: int = None,
    ) -> list[StoredEvent]:
        with self.database_lock:
            results = []
            index = self.stored_events_index[originator_id]
            positions = index.keys()
            if desc:
                positions = reversed(list(positions))
            for position in positions:
                if gt and not position > gt:
                    continue
                if lte and not position <= lte:
                    continue
                stored_event = self.stored_events[index[position]]
                results.append(stored_event)
                if len(results) == limit:
                    break
            return results
