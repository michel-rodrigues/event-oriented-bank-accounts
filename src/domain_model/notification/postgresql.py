import uuid
from typing import Sequence

import psycopg2
from psycopg2.extensions import cursor

from src.domain_model.recorders.postgresql import PostgresAggregateRecorder
from src.patterns.domain_model.notification import ApplicationRecorder, Notification


class PostgresApplicationRecorder(ApplicationRecorder, PostgresAggregateRecorder):
    def _create_table(self, cursor: cursor):
        super()._create_table(cursor)
        statement = (
            f'ALTER TABLE {self._events_table} '
            'ADD COLUMN IF NOT EXISTS '
            f'rowid SERIAL'  # fmt: skip
        )
        try:
            cursor.execute(statement)
        except psycopg2.OperationalError as error:
            raise self.OperationalError(error) from error
        statement = (
            'CREATE UNIQUE INDEX IF NOT EXISTS rowid_idx '
            f'ON {self._events_table} (rowid ASC);'  # fmt: skip
        )
        try:
            cursor.execute(statement)
        except psycopg2.OperationalError as error:
            raise self.OperationalError(error) from error

    def _build_notification(self, row):
        return Notification(
            id=row['rowid'],
            originator_id=uuid.UUID(row['originator_id']),
            originator_version=row['originator_version'],
            topic=row['topic'],
            state=bytes(row['state']),
        )

    def select_notifications(self, start: int, limit: int) -> Sequence[Notification]:
        """Returns a list of event notifications
        from 'start', limited by 'limit'.
        """
        statement = (
            'SELECT * '
            f'FROM {self._events_table} '
            'WHERE rowid>=%s '
            'ORDER BY rowid '
            'LIMIT %s'  # fmt: skip
        )
        with self._db.transaction() as cursor:
            cursor.execute(statement, [start, limit])
            return [self._build_notification(row) for row in cursor.fetchall()]

    def max_notification_id(self) -> int:
        """Returns the maximum notification ID."""
        cursor = self._db.get_connection().cursor()
        statement = f'SELECT MAX(rowid) FROM {self._events_table}'
        cursor.execute(statement)
        return cursor.fetchone()[0] or 0
