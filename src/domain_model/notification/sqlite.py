import uuid
from typing import Sequence

from src.domain_model.recorders.sqlite import SQLiteAggregateRecorder
from src.patterns.domain_model.notification import ApplicationRecorder, Notification


class SQLiteApplicationRecorder(ApplicationRecorder, SQLiteAggregateRecorder):
    def _build_notification(self, row):
        return Notification(
            id=row['rowid'],
            originator_id=uuid.UUID(row['originator_id']),
            originator_version=row['originator_version'],
            topic=row['topic'],
            state=row['state'],
        )

    def select_notifications(self, start: int, limit: int) -> Sequence[Notification]:
        """Returns a list of event notifications
        from 'start', limited by 'limit'.
        """
        statement = (
            'SELECT rowid, *'
            f'FROM {self._table_name} '
            'WHERE rowid>=? '
            'ORDER BY rowid '
            'LIMIT ?'  # fmt: skip
        )
        cursor = self._db.get_connection().cursor()
        cursor.execute(statement, [start, limit])
        return [self._build_notification(row) for row in cursor.fetchall()]

    def max_notification_id(self) -> int:
        """Returns the maximum notification ID."""
        cursor = self._db.get_connection().cursor()
        statement = f'SELECT MAX(rowid) FROM {self._table_name}'
        cursor.execute(statement)
        return cursor.fetchone()[0] or 0
