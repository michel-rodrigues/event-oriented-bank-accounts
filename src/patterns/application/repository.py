import uuid
from typing import Union

from src.patterns.application.snapshot import Snapshot
from src.patterns.domain_model.aggregate import Aggregate
from src.patterns.domain_model.event_store import EventStore


class Repository:
    def __init__(self, event_store: EventStore, snapshot_store: EventStore = None):
        self._event_store = event_store
        self._snapshot_store = snapshot_store

    def _fetch_snapshot(self, aggregate_id, version):
        snapshots = self._snapshot_store.get(originator_id=aggregate_id, desc=True, limit=1, lte=version)
        try:
            return next(snapshots)
        except StopIteration:
            pass

    def _project(self, domain_events: list[Union[Snapshot, Aggregate.Event]]):
        aggregate = None
        for domain_event in domain_events:
            aggregate = domain_event.mutate(aggregate)
        return aggregate

    def get(self, aggregate_id: uuid.UUID, version: int = None) -> Aggregate:
        gt = None
        domain_events = []
        # Try to get a snapshot.
        if self._snapshot_store and (snapshot := self._fetch_snapshot(aggregate_id, version)):
            gt = snapshot.originator_version
            domain_events.append(snapshot)
        # Get the domain events.
        domain_events += self._event_store.get(originator_id=aggregate_id, gt=gt, lte=version)
        # Project the domain events.
        aggregate = self._project(domain_events)
        # Raise exception if not found.
        if not aggregate:
            raise AggregateNotFoundError
        # Return the aggregate.
        assert isinstance(aggregate, Aggregate)
        return aggregate


class AggregateNotFoundError(Exception):
    pass
