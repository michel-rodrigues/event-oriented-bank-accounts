from copy import deepcopy
from datetime import datetime

from src.patterns.domain_model.aggregate import Aggregate, get_topic, resolve_topic


class Snapshot(Aggregate.Event):
    topic: str
    state: dict

    @classmethod
    def take(cls, aggregate: Aggregate) -> Aggregate.Event:
        return cls(
            originator_id=aggregate.id,
            originator_version=aggregate.version,
            timestamp=datetime.now(),
            topic=get_topic(type(aggregate)),
            state=deepcopy(vars(aggregate)),
        )

    def mutate(self, *args, **kwargs) -> Aggregate:
        cls = resolve_topic(self.topic)
        aggregate = object.__new__(cls)
        assert isinstance(aggregate, Aggregate)
        vars(aggregate).update(self.state)
        return aggregate
