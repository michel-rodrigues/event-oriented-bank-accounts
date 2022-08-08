import abc
import uuid
from typing import Sequence

from src.patterns.domain_model.mapper import StoredEvent


class Recorder(abc.ABC):
    class OperationalError(Exception):
        pass

    class IntegrityError(Exception):
        pass


class AggregateRecorder(Recorder):
    @abc.abstractmethod
    def insert_events(self, stored_events: Sequence[StoredEvent], **kwargs) -> None:
        """Writes stored events into database."""

    @abc.abstractmethod
    def select_events(
        self,
        originator_id: uuid.UUID,
        gt: int = None,
        lte: int = None,
        desc: bool = False,
        limit: int = None,
    ) -> Sequence[StoredEvent]:
        """Reads stored events from database."""
