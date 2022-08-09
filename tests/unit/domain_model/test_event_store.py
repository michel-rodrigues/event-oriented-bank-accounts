import zlib
from decimal import Decimal

from src.domain_model.aggregate import BankAccount
from src.domain_model.mapper.cipher import SECRET_KEY, Cipher
from src.domain_model.recorders import InMemoryAggregateRecorder
from src.patterns.domain_model.event_store import EventStore
from src.patterns.domain_model.mapper import Mapper


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
