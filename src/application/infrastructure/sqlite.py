from distutils.util import strtobool
from threading import Lock

from src.domain_model.notification.sqlite import SQLiteApplicationRecorder
from src.domain_model.recorders.sqlite import SQLiteAggregateRecorder, SQLiteDatabase
from src.patterns.application_layer.application.infrastructure_factory import InfrastructureFactory
from src.patterns.domain_model_layer.notification import ApplicationRecorder
from src.patterns.domain_model_layer.recorder import AggregateRecorder


class SQLiteInfrastructureFactory(InfrastructureFactory):
    DB_URI = 'DB_URI'
    CREATE_TABLE = 'CREATE_TABLE'

    def __init__(self, application_name):
        super().__init__(application_name)
        self._database = None
        self.lock = Lock()  # Where this is going to be used?

    @property
    def database(self):
        with self.lock:
            if self._database is None:
                db_uri = self.getenv(self.DB_URI, ':memory:')
                if not db_uri:
                    raise EnvironmentError('Database URI not found in environment with key "{self.DB_URI}"')
                self._database = SQLiteDatabase(db_uri=db_uri)
            return self._database

    def aggregate_recorder(self) -> AggregateRecorder:
        recorder = SQLiteAggregateRecorder(db=self._database)
        if self.do_create_table():
            recorder.create_table()
        return recorder

    def application_recorder(self) -> ApplicationRecorder:
        recorder = SQLiteApplicationRecorder(db=self._database)
        if self.do_create_table():
            recorder.create_table()
        return recorder

    # def process_recorder(self) -> ProcessRecorder:
    #     recorder = SQLiteProcessRecorder(db=self.database)
    #     if self.do_create_table():
    #         recorder.create_table()
    #     return recorder

    def do_create_table(self) -> bool:
        default = 'no'
        return bool(strtobool(self.getenv(self.CREATE_TABLE, default) or default))
