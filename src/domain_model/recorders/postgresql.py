import os
import threading
import uuid
from typing import Sequence

import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import DictCursor

from src.patterns.domain_model.mapper import StoredEvent
from src.patterns.domain_model.recorder import AggregateRecorder


class PostgresDatabase:
    def __init__(self):
        self._connections = {}

    def get_connection(self) -> connection:
        thread_id = threading.get_ident()
        try:
            return self._connections[thread_id]
        except KeyError:
            connection = self.create_connection()
        self._connections[thread_id] = connection
        return connection

    def create_connection(self) -> connection:
        return psycopg2.connect(
            dbname=os.environ['POSTGRES_DBNAME'],
            host=os.environ['POSTGRES_HOST'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
        )

    class Transaction:
        def __init__(self, connection: connection):
            self._connection = connection

        def __enter__(self) -> cursor:
            cursor = self._connection.cursor(cursor_factory=DictCursor)
            return cursor

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type:
                # Roll back all changes if an exception occurs.
                self._connection.rollback()
            else:
                self._connection.commit()

    def transaction(self) -> Transaction:
        return self.Transaction(self.get_connection())


class PostgresAggregateRecorder(AggregateRecorder):
    def __init__(self, application_name: str = ''):
        self._db = PostgresDatabase()
        self._application_name = application_name
        self._events_table = application_name.lower() + 'events'

    def create_table(self):
        with self._db.transaction() as cursor:
            self._create_table(cursor)

    def _create_table(self, cursor: cursor):
        statement = (
            'CREATE TABLE IF NOT EXISTS '
            f'{self._events_table} ('
            'originator_id uuid NOT NULL, '
            'originator_version integer NOT NULL, '
            'topic text, '
            'state bytea, '
            'PRIMARY KEY (originator_id, originator_version));'
            # Don't truncate the table in production,
            # this line is just to keep testing independent
            f'TRUNCATE TABLE {self._events_table} RESTART IDENTITY;'
        )
        try:
            cursor.execute(statement)
        except psycopg2.OperationalError as error:
            raise self.OperationalError(error) from error

    def insert_events(self, stored_events: Sequence[StoredEvent], **kwargs):
        with self._db.transaction() as cursor:
            self._insert_events(cursor, stored_events, **kwargs)

    def _build_row(self, stored_event: StoredEvent) -> tuple:
        return stored_event.originator_id.hex, stored_event.originator_version, stored_event.topic, stored_event.state

    def _insert_events(self, cursor: cursor, stored_events: Sequence[StoredEvent], **kwargs) -> None:
        params = (self._build_row(stored_event) for stored_event in stored_events)
        try:
            cursor.executemany(f'INSERT INTO {self._events_table} VALUES (%s, %s, %s, %s)', params)
        except psycopg2.IntegrityError as error:
            raise self.IntegrityError(error) from error

    def _build_store_event(self, row: DictCursor):
        return StoredEvent(
            originator_id=uuid.UUID(row['originator_id']),
            originator_version=row['originator_version'],
            topic=row['topic'],
            state=bytes(row['state']),
        )

    def select_events(
        self,
        originator_id: uuid.UUID,
        gt: int = None,
        lte: int = None,
        desc: bool = False,
        limit: int = None,
    ) -> Sequence[StoredEvent]:
        statement = f'SELECT * FROM {self._events_table} WHERE originator_id = %s'
        params = [originator_id.hex]
        if gt:
            statement += ' AND originator_version > %s'
            params.append(gt)
        if lte:
            statement += ' AND originator_version <= %s'
            params.append(lte)
        statement += ' ORDER BY originator_version'
        if desc:
            statement += ' DESC'
        else:
            statement += ' ASC'
        if limit:
            statement += ' LIMIT %s'
            params.append(limit)
        with self._db.transaction() as cursor:
            cursor.execute(statement, params)
            return [self._build_store_event(row) for row in cursor.fetchall()]
