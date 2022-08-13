import uuid
import zlib
from decimal import Decimal

import pytest

from src.domain_model.aggregate import BankAccount
from src.domain_model.mapper.cipher import SECRET_KEY, Cipher
from src.domain_model.recorders.in_memory import InMemoryAggregateRecorder
from src.patterns.application.repository import AggregateNotFoundError, Repository
from src.patterns.application.snapshot import Snapshot
from src.patterns.domain_model.event_store import EventStore
from src.patterns.domain_model.mapper import Mapper


def test_it_should_project_the_state_from_stored_events(json_transcoder):
    event_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    account = BankAccount.open(full_name='Steve Magal', email_address='steve.magal@irmaodojorel.com')
    account.append_transaction(Decimal('10.00'))
    account.append_transaction(Decimal('25.00'))
    account.append_transaction(Decimal('30.00'))
    pending = account._collect_()
    event_store.put(pending)

    repository = Repository(event_store)
    retrieved_account: BankAccount = repository.get(account.id)
    assert retrieved_account.id == account.id
    assert retrieved_account.balance == account.balance


def test_it_should_project_the_state_from_snapshot(json_transcoder):
    event_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    snapshot_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    account = BankAccount.open(full_name='Steve Magal', email_address='steve.magal@irmaodojorel.com')
    account.append_transaction(Decimal('10.00'))
    account.append_transaction(Decimal('25.00'))
    account.append_transaction(Decimal('30.00'))
    account.pending_events.clear()
    snapshot = Snapshot.take(account)
    snapshot_store.put([snapshot])

    repository = Repository(event_store, snapshot_store)
    retrieved_account: BankAccount = repository.get(account.id)
    assert retrieved_account.id == account.id
    assert retrieved_account.balance == account.balance


def test_it_should_project_the_state_from_snapshot_and_stored_events(json_transcoder):
    event_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    snapshot_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    account = BankAccount.open(full_name='Steve Magal', email_address='steve.magal@irmaodojorel.com')
    account.append_transaction(Decimal('10.00'))
    account.append_transaction(Decimal('25.00'))
    account.pending_events.clear()
    snapshot = Snapshot.take(account)
    snapshot_store.put([snapshot])

    account.append_transaction(Decimal('30.00'))
    pending = account._collect_()
    event_store.put(pending)

    repository = Repository(event_store, snapshot_store)
    retrieved_account: BankAccount = repository.get(account.id)
    assert retrieved_account.id == account.id
    assert retrieved_account.balance == account.balance


def test_it_should_retrive_agreggate_with_the_state_referring_to_the_provided_version(json_transcoder):
    event_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    snapshot_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    account = BankAccount.open(full_name='Steve Magal', email_address='steve.magal@irmaodojorel.com')
    account.append_transaction(Decimal('10.00'))
    pending = account._collect_()
    event_store.put(pending)

    account.append_transaction(Decimal('25.00'))
    account.pending_events.clear()
    snapshot = Snapshot.take(account)
    snapshot_store.put([snapshot])

    account.append_transaction(Decimal('30.00'))
    pending = account._collect_()
    event_store.put(pending)

    repository = Repository(event_store, snapshot_store)
    retrieved_account: BankAccount = repository.get(account.id, version=1)
    assert retrieved_account.id == account.id
    assert retrieved_account.balance == Decimal('0.00')

    retrieved_account: BankAccount = repository.get(account.id, version=2)
    assert retrieved_account.id == account.id
    assert retrieved_account.balance == Decimal('10.00')

    retrieved_account: BankAccount = repository.get(account.id, version=3)
    assert retrieved_account.id == account.id
    assert retrieved_account.balance == Decimal('35.00')

    retrieved_account: BankAccount = repository.get(account.id, version=4)
    assert retrieved_account.id == account.id
    assert retrieved_account.balance == Decimal('65.00')


def test_it_should_raise_an_error_when_there_is_no_stored_event_related_to_the_aggregate(json_transcoder):
    event_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    snapshot_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    repository = Repository(event_store, snapshot_store)
    with pytest.raises(AggregateNotFoundError):
        repository.get(uuid.uuid4())
