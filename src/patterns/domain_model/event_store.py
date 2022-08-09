import uuid
from typing import Iterator, Sequence

from src.patterns.domain_model.aggregate import Aggregate
from src.patterns.domain_model.mapper import Mapper
from src.patterns.domain_model.recorder import AggregateRecorder


class EventStore:
    """Stores and retrieves domain events."""

    def __init__(self, mapper: Mapper, recorder: AggregateRecorder):
        self._mapper = mapper
        self._recorder = recorder

    def put(self, events: Sequence[Aggregate.Event], **kwargs):
        """Stores domain events in aggregate sequence."""
        self._recorder.insert_events([self._mapper.from_event(event) for event in events], **kwargs)

    def get(
        self,
        originator_id: uuid.UUID,
        gt: int = None,
        lte: int = None,
        desc: bool = False,
        limit: int = None,
    ) -> Iterator[Aggregate.Event]:
        """Retrieves domain events from aggregate sequence."""
        stored_events = self._recorder.select_events(
            originator_id=originator_id,
            gt=gt,
            lte=lte,
            desc=desc,
            limit=limit,
        )
        return iter(self._mapper.to_event(stored_event) for stored_event in stored_events)
