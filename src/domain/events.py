from src.patterns import DomainEvent


class AccountOpened(DomainEvent):
    full_name: str


class FullNameUpdated(DomainEvent):
    full_name: str


class AccountClosed(DomainEvent):
    pass