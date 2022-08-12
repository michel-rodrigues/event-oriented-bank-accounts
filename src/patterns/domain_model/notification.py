import abc
from typing import Sequence

from src.patterns.domain_model.mapper import StoredEvent
from src.patterns.domain_model.recorder import AggregateRecorder


class Notification(StoredEvent):
    id: int


class ApplicationRecorder(AggregateRecorder):
    @abc.abstractmethod
    def select_notifications(self, start: int, limit: int) -> Sequence[Notification]:
        """Returns a sequence of event notifications from 'start',
        limited by 'limit'.
        """

    @abc.abstractmethod
    def max_notification_id(self) -> int:
        """Returns the maximum notification ID."""
