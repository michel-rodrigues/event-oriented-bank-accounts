import zlib
from decimal import Decimal

import pytest

from src.domain_model.aggregate import BankAccount
from src.domain_model.mapper.cipher import SECRET_KEY, Cipher
from src.domain_model.recorders.in_memory import InMemoryAggregateRecorder
from src.patterns.domain_model_layer.event_store import EventStore
from src.patterns.domain_model_layer.mapper import Mapper
from src.patterns.domain_model_layer.recorder import Recorder


def test_it_should_store_and_retrieve_the_events(json_transcoder):
    event_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    # Open an account.
    account = BankAccount.open(full_name='Steve Magal', email_address='steve.magal@irmaodojorel.com')
    # Credit the account.
    account.append_transaction(Decimal('10.00'))
    account.append_transaction(Decimal('25.00'))
    account.append_transaction(Decimal('30.00'))
    # Collect pending events.
    pending = account._collect_()
    # Store pending events.
    event_store.put(pending)
    # Get domain events.
    events = event_store.get(account.id)

    # Reconstruct the bank account.
    current_aggregate_state = None
    for event in events:
        current_aggregate_state = event.mutate(current_aggregate_state)

    assert current_aggregate_state.id == account.id
    assert current_aggregate_state.balance == Decimal('65.00')


def test_it_should_raise_an_error_when_attempt_to_store_an_event_with_old_version(json_transcoder):
    event_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=InMemoryAggregateRecorder(),
    )
    account = BankAccount.open(full_name='Steve Magal', email_address='steve.magal@irmaodojorel.com')
    account.append_transaction(Decimal('10.00'))
    account.append_transaction(Decimal('25.00'))
    pending = account._collect_()
    event_store.put(pending)

    event_version_2 = list(pending)[1]

    with pytest.raises(Recorder.IntegrityError):
        event_store.put([event_version_2])
