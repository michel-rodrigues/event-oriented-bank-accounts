import zlib
from decimal import Decimal

from src.domain_model.aggregate import BankAccount
from src.domain_model.mapper.cipher import SECRET_KEY, Cipher
from src.domain_model.recorders.sqlite import SQLiteAggregateRecorder
from src.patterns.application_layer.snapshot import Snapshot
from src.patterns.domain_model_layer.event_store import EventStore
from src.patterns.domain_model_layer.mapper import Mapper


def test_take_a_snapshot_of_the_current_aggregate_state(json_transcoder):
    snapshot_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=SQLiteAggregateRecorder(table_name='snapshots'),
    )
    snapshot_store._recorder.create_table()
    account = BankAccount.open(full_name='Steve Magal', email_address='steve.magal@irmaodojorel.com')
    account.append_transaction(Decimal('10.00'))
    account.append_transaction(Decimal('25.00'))
    account.append_transaction(Decimal('30.00'))
    # Clear pending events.
    account.pending_events.clear()
    # Take a snapshot.
    snapshot = Snapshot.take(account)
    assert snapshot.state['id'] == account.id
    assert snapshot.state['balance'] == account.balance
    assert snapshot.state['full_name'] == account.full_name
    assert snapshot.state['email_address'] == account.email_address
    assert snapshot.state['overdraft_limit'] == account.overdraft_limit
    assert snapshot.state['is_closed'] == account.is_closed


def test_reconstruct_the_aggregate_from_snapshot(json_transcoder):
    snapshot_store = EventStore(
        mapper=Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder),
        recorder=SQLiteAggregateRecorder(table_name='snapshots'),
    )
    snapshot_store._recorder.create_table()
    account = BankAccount.open(full_name='Steve Magal', email_address='steve.magal@irmaodojorel.com')
    account.append_transaction(Decimal('10.00'))
    account.append_transaction(Decimal('25.00'))
    account.append_transaction(Decimal('30.00'))
    # Clear pending events.
    account.pending_events.clear()
    # Take a snapshot.
    snapshot = Snapshot.take(account)
    # Store snapshot.
    snapshot_store.put([snapshot])
    # Ordering by originator_version get the most recent snapshot.
    snapshot = next(snapshot_store.get(account.id, desc=True, limit=1))
    # Reconstruct the aggregate.
    account_from_snapshot: BankAccount = snapshot.mutate()
    assert account_from_snapshot.id == account.id
    assert account_from_snapshot.balance == account.balance
    assert account_from_snapshot.full_name == account.full_name
    assert account_from_snapshot.email_address == account.email_address
    assert account_from_snapshot.overdraft_limit == account.overdraft_limit
    assert account_from_snapshot.is_closed == account.is_closed
