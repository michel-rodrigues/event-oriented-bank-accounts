from typing import Sequence

from src.domain_model.recorders.in_memory import InMemoryAggregateRecorder
from src.patterns.domain_model_layer.notification import ApplicationRecorder, Notification


class InMemoryApplicationRecorder(ApplicationRecorder, InMemoryAggregateRecorder):
    def _build_notification(self, notification_id, stored_event):
        return Notification(
            id=notification_id,
            originator_id=stored_event.originator_id,
            originator_version=stored_event.originator_version,
            topic=stored_event.topic,
            state=stored_event.state,
        )

    def select_notifications(self, start: int, limit: int) -> Sequence[Notification]:
        zero_based_adjusted_start = start - 1
        last_limit_position = zero_based_adjusted_start + limit
        selection = slice(zero_based_adjusted_start, last_limit_position)
        with self._database_lock:
            indexed_stored_events = enumerate(self._stored_events[selection], start)
            return [self._build_notification(*indexed_stored_event) for indexed_stored_event in indexed_stored_events]

    def max_notification_id(self) -> int:
        with self._database_lock:
            return len(self._stored_events)
