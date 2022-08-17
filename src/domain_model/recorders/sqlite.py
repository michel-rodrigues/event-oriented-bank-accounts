import sqlite3
import threading
import uuid
from typing import Sequence

from src.patterns.domain_model_layer.recorder import AggregateRecorder, StoredEvent


class SQLiteDatabase:
    def __init__(self, db_uri):
        self._db_uri = db_uri
        self._connections = {}

    class Transaction:
        def __init__(self, connection: sqlite3.Connection):
            self._connection = connection

        def __enter__(self) -> sqlite3.Connection:
            # We must issue a "BEGIN" explicitly
            # when running in auto-commit mode.
            self._connection.execute('BEGIN')
            return self._connection

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type:
                # Roll back all changes if an exception occurs.
                self._connection.rollback()
            else:
                self._connection.commit()

    def transaction(self) -> Transaction:
        return self.Transaction(self.get_connection())

    def get_connection(self) -> sqlite3.Connection:
        thread_id = threading.get_ident()
        try:
            return self._connections[thread_id]
        except KeyError:
            connection = self.create_connection()
            self._connections[thread_id] = connection
        return connection

    def create_connection(self) -> sqlite3.Connection:
        # Make a connection to an SQLite database.
        connection = sqlite3.connect(
            database=self._db_uri,
            uri=True,
            check_same_thread=False,
            isolation_level=None,  # Auto-commit mode.
            cached_statements=True,
        )
        connection.row_factory = sqlite3.Row
        # Use WAL (write-ahead log) mode.
        connection.execute('pragma journal_mode=wal;')
        return connection


class SQLiteAggregateRecorder(AggregateRecorder):
    def __init__(self, table_name: str = 'stored_events', db: SQLiteDatabase = None):
        self._db = db or SQLiteDatabase(':memory:')
        self._table_name = table_name

    def create_table(self):
        with self._db.transaction() as connection:
            self._create_table(connection)
            # Don't truncate the table in production,
            # this line is just to keep testing independent
            self._truncate_table(connection)

    def _create_table(self, connection: sqlite3.Connection):
        statement = (
            f'CREATE TABLE IF NOT EXISTS {self._table_name} ('
            'originator_id TEXT, '
            'originator_version INTEGER, '
            'topic TEXT, '
            'state BLOB, '
            'PRIMARY KEY (originator_id, originator_version))'
        )
        try:
            connection.execute(statement)
        except sqlite3.OperationalError as error:
            raise self.OperationalError(error)

    def _truncate_table(self, connection: sqlite3.Connection):
        try:
            connection.execute(f'DELETE FROM {self._table_name}')
        except sqlite3.OperationalError as error:
            raise self.OperationalError(error)

    def insert_events(self, stored_events: Sequence[StoredEvent], **kwargs):
        with self._db.transaction() as connection:
            self._insert_events(connection, stored_events, **kwargs)

    def _build_row(self, stored_event: StoredEvent) -> tuple:
        return stored_event.originator_id.hex, stored_event.originator_version, stored_event.topic, stored_event.state

    def _insert_events(self, connection: sqlite3.Connection, stored_events: Sequence[StoredEvent], **kwargs) -> None:
        params = (self._build_row(stored_event) for stored_event in stored_events)
        try:
            connection.executemany(f'INSERT INTO {self._table_name} VALUES (?,?,?,?)', params)
        except sqlite3.IntegrityError as error:
            raise self.IntegrityError(error) from error

    def _build_store_event(self, row: sqlite3.Row):
        return StoredEvent(
            originator_id=uuid.UUID(row['originator_id']),
            originator_version=row['originator_version'],
            topic=row['topic'],
            state=row['state'],
        )

    def select_events(
        self,
        originator_id: uuid.UUID,
        gt: int = None,
        lte: int = None,
        desc: bool = False,
        limit: int = None,
    ) -> Sequence[StoredEvent]:
        statement = f'SELECT * FROM {self._table_name} WHERE originator_id=?'
        params = [originator_id.hex]
        if gt:
            statement += ' AND originator_version>?'
            params.append(gt)
        if lte:
            statement += ' AND originator_version<=?'
            params.append(lte)
        statement += ' ORDER BY originator_version'
        if desc:
            statement += ' DESC'
        else:
            statement += ' ASC'
        if limit:
            statement += ' LIMIT ?'
            params.append(limit)
        connection = self._db.get_connection()
        return [self._build_store_event(row) for row in connection.execute(statement, params)]
