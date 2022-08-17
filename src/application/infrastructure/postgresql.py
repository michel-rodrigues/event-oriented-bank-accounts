from distutils.util import strtobool
from threading import Lock

from src.domain_model.notification.postgresql import PostgresApplicationRecorder
from src.domain_model.recorders.postgresql import PostgresAggregateRecorder
from src.patterns.application_layer.application.infrastructure_factory import InfrastructureFactory
from src.patterns.domain_model_layer.notification import ApplicationRecorder
from src.patterns.domain_model_layer.recorder import AggregateRecorder


class PostgresInfrastructureFactory(InfrastructureFactory):

    CREATE_TABLE = 'CREATE_TABLE'

    def __init__(self, application_name):
        super().__init__(application_name)
        self.lock = Lock()  # Where this is going to be used?

    def aggregate_recorder(self) -> AggregateRecorder:
        recorder = PostgresAggregateRecorder(self.application_name)
        if self.do_create_table():
            recorder.create_table()
        return recorder

    def application_recorder(self) -> ApplicationRecorder:
        recorder = PostgresApplicationRecorder(self.application_name)
        if self.do_create_table():
            recorder.create_table()
        return recorder

    # def process_recorder(self) -> ProcessRecorder:
    #     recorder = PostgresProcessRecorder(self.application_name)
    #     if self.do_create_table():
    #         recorder.create_table()
    #     return recorder

    def do_create_table(self) -> bool:
        default = 'no'
        return bool(strtobool(self.getenv(self.CREATE_TABLE, default) or default))
