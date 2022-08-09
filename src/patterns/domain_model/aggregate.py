import importlib
import uuid
from copy import deepcopy
from datetime import datetime
from typing import Sequence

from src.patterns.domain_model.domain_event import DomainEvent


class Aggregate:
    """Base class for aggregate roots."""

    class Event(DomainEvent):
        """Base domain event class for aggregates."""

        def mutate(self, obj: 'Aggregate' = None) -> 'Aggregate':
            """Changes the state of the aggregate
            according to domain event attributes.
            """
            assert isinstance(obj, Aggregate)
            # Use counting to follow the sequence.
            next_version = obj.version + 1
            # Check event is next in its sequence.
            if self.originator_version != next_version:
                raise obj.VersionError(self.originator_version, next_version)
            # Update the aggregate version.
            obj.version = next_version
            # Update the modified time.
            obj.modified_on = self.timestamp
            self.apply(obj)
            return obj

        def apply(self, obj) -> None:
            pass

    class VersionError(Exception):
        pass

    def __init__(self, id: uuid.UUID, version: int, timestamp: datetime):
        """Aggregate is constructed with an 'id' and a 'version'.
        The 'pending_events' is also initialised as an empty list.
        """
        self.id = id
        self.version = version
        self.created_on = timestamp
        self.modified_on = timestamp
        self.pending_events: list[Aggregate.Event] = []

    class Created(Event):
        """Domain event for when aggregate is created."""

        originator_topic: str

        def mutate(self, obj: 'Aggregate') -> 'Aggregate':
            """Constructs aggregate instance defined by
            domain event object attributes.
            """
            # Copy the event attributes.
            kwargs = deepcopy(vars(self))
            # Separate the id and version.
            id = kwargs.pop('originator_id')
            version = kwargs.pop('originator_version')
            # Get the aggregate root class from topic.
            aggregate_class = resolve_topic(kwargs.pop('originator_topic'))
            # Construct and return aggregate object.
            return aggregate_class(id=id, version=version, **kwargs)

    @classmethod
    def _create_(cls, created_event_class: Created, **kwargs):
        """Factory method to construct a new aggregate object instance."""
        # Construct the domain event class, with an ID and version, and the
        # a topic for the aggregate class.
        event: Aggregate.Created = created_event_class(
            originator_id=uuid.uuid4(),
            originator_version=1,
            originator_topic=get_topic(cls),
            timestamp=datetime.now(),
            **kwargs,
        )
        # Construct the aggregate object.
        aggregate = event.mutate(None)
        # Append the domain event to pending list.
        aggregate.pending_events.append(event)
        # Return the aggregate.
        return aggregate

    def _trigger_(self, event_class: Event, **kwargs) -> None:
        """Triggers domain event of given type, extending the sequence of
        domain events for this aggregate object.
        """
        # Construct the domain event as the next in the aggregate's sequence.
        # Use counting to generate the sequence.
        next_version = self.version + 1
        event: Aggregate.Event = event_class(
            originator_id=self.id,
            originator_version=next_version,
            timestamp=datetime.now(),
            **kwargs,
        )
        # Mutate aggregate with domain event.
        event.mutate(self)
        # Append the domain event to pending list.
        self.pending_events.append(event)

    def _collect_(self) -> Sequence[Event]:
        """Collects pending events."""
        return self.pending_events


def get_topic(cls: type) -> str:
    """Returns a string that locates the given class."""
    return f'{cls.__module__}#{cls.__qualname__}'


def resolve_topic(topic: str) -> type:
    """Returns a class located by the given string."""
    module_name, _, class_name = topic.partition('#')
    module = importlib.import_module(module_name)
    return resolve_attr(module, class_name)


def resolve_attr(obj, path: str) -> type:
    if not path:
        return obj
    head, _, tail = path.partition('.')
    obj = getattr(obj, head)
    return resolve_attr(obj, tail)
