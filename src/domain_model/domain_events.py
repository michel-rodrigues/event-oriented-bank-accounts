from src.patterns.domain_model_layer.domain_event import DomainEvent


class AccountOpened(DomainEvent):
    full_name: str


class FullNameUpdated(DomainEvent):
    full_name: str


class AccountClosed(DomainEvent):
    pass
