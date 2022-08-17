from src.domain_model.notification.in_memory import InMemoryApplicationRecorder
from src.domain_model.recorders.in_memory import InMemoryAggregateRecorder
from src.patterns.application_layer.application.infrastructure_factory import InfrastructureFactory
from src.patterns.domain_model_layer.notification import ApplicationRecorder
from src.patterns.domain_model_layer.recorder import AggregateRecorder


class InMemoryInfrastructureFactory(InfrastructureFactory):
    def aggregate_recorder(self) -> AggregateRecorder:
        return InMemoryAggregateRecorder()

    def application_recorder(self) -> ApplicationRecorder:
        return InMemoryApplicationRecorder()

    # def process_recorder(self) -> ProcessRecorder:
    #     return InMemoryProcessRecorder()
