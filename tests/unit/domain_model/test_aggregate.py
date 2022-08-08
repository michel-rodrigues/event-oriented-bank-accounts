import uuid
from datetime import datetime
from decimal import Decimal

import pytest

from src.domain_model.aggregate import AccountClosedError, BankAccount, InsufficientFundsError


def test_open_a_bank_account():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    assert account.full_name == 'Alice'
    assert account.email_address == 'alice@example.com'
    assert account.balance == 0
    assert account.overdraft_limit == 0
    assert account.is_closed is False


def test_open_a_bank_account_should_emit_an_opened_event():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    pending = account._collect_()
    assert len(pending) == 1
    opened_event = pending[0]
    assert isinstance(opened_event, BankAccount.Opened)
    assert opened_event.email_address == 'alice@example.com'
    assert opened_event.full_name == 'Alice'
    assert opened_event.originator_version == 1
    assert opened_event.originator_topic == 'src.domain_model.aggregate#BankAccount'
    assert isinstance(opened_event.originator_id, uuid.UUID)
    assert isinstance(opened_event.timestamp, datetime)


def test_append_a_credit_transaction_should_be_added_to_the_balance():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.append_transaction(Decimal('10.00'))
    assert account.balance == Decimal('10.00')


def test_append_a_debt_transaction_should_be_subtracted_to_the_balance():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.append_transaction(Decimal('15.00'))
    account.append_transaction(Decimal('-5.00'))
    assert account.balance == Decimal('10.00')


def test_append_a_debt_transaction_with_insufficient_funds_should_raise_an_error():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.append_transaction(Decimal('10.00'))
    with pytest.raises(InsufficientFundsError):
        account.append_transaction(Decimal('-25.00'))


def test_append_a_transaction_should_emit_a_transaction_appended_event():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.append_transaction(Decimal('10.00'))
    pending = account._collect_()
    assert len(pending) == 2
    transaction_appended_event = pending[1]
    assert isinstance(transaction_appended_event, BankAccount.TransactionAppended)
    assert transaction_appended_event.amount == Decimal('10.00')
    assert transaction_appended_event.originator_version == 2
    assert isinstance(transaction_appended_event.originator_id, uuid.UUID)
    assert isinstance(transaction_appended_event.timestamp, datetime)


def test_it_should_not_emit_a_transaction_appended_event_when_fail_to_append_a_transaction():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    with pytest.raises(InsufficientFundsError):
        account.append_transaction(Decimal('-5.00'))
    pending = account._collect_()
    assert len(pending) == 1
    assert isinstance(pending[0], BankAccount.Opened)


def test_increase_the_overdraft_limit():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    assert account.overdraft_limit == Decimal('0.00')
    account.set_overdraft_limit(Decimal('10.00'))
    assert account.overdraft_limit == Decimal('10.00')


def test_increasing_the_overdraft_limit_allow_append_a_debt_transaction_which_negativate_the_balance():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.set_overdraft_limit(Decimal('10.00'))
    account.append_transaction(Decimal('-10.00'))
    assert account.balance == Decimal('-10.00')


def test_decrease_the_overdraft_limit_is_not_allowed():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    with pytest.raises(AssertionError):
        account.set_overdraft_limit(Decimal('-1.00'))


def test_it_should_not_append_a_debt_transaction_which_exceed_the_overdraft_limit():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.append_transaction(Decimal('5.00'))
    account.set_overdraft_limit(Decimal('9.00'))
    with pytest.raises(InsufficientFundsError):
        account.append_transaction(Decimal('-15.00'))


def test_increase_the_overdraft_limit_should_emit_an_overdraft_limit_set_event():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.set_overdraft_limit(Decimal('10.00'))
    pending = account._collect_()
    assert len(pending) == 2
    transaction_appended_event = pending[1]
    assert isinstance(transaction_appended_event, BankAccount.OverdraftLimitSet)
    assert transaction_appended_event.overdraft_limit == Decimal('10.00')
    assert transaction_appended_event.originator_version == 2
    assert isinstance(transaction_appended_event.originator_id, uuid.UUID)
    assert isinstance(transaction_appended_event.timestamp, datetime)


def test_it_should_not_emit_an_overdraft_limit_set_when_fail_to_set_the_overdraft_limit():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    with pytest.raises(AssertionError):
        account.set_overdraft_limit(Decimal('-1.00'))
    pending = account._collect_()
    assert len(pending) == 1
    assert isinstance(pending[0], BankAccount.Opened)


def test_close_an_account():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.close()
    assert account.is_closed is True


def test_close_an_account_should_emit_a_closed_event():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.close()
    pending = account._collect_()
    assert len(pending) == 2
    transaction_appended_event = pending[1]
    assert isinstance(transaction_appended_event, BankAccount.Closed)
    assert transaction_appended_event.originator_version == 2
    assert isinstance(transaction_appended_event.originator_id, uuid.UUID)
    assert isinstance(transaction_appended_event.timestamp, datetime)


def test_it_should_not_append_transaction_to_a_closed_account():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.close()
    with pytest.raises(AccountClosedError):
        account.append_transaction(Decimal('5.00'))


def test_it_should_increase_the_overdraft_limit_of_a_closed_account():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.close()
    with pytest.raises(AccountClosedError):
        account.set_overdraft_limit(Decimal('9.00'))


def test_events_emitted_by_the_same_runtime_instance_should_be_accumulated():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.append_transaction(Decimal('5.00'))
    account.set_overdraft_limit(Decimal('10.00'))
    account.close()
    pending = account._collect_()
    assert len(pending) == 4


def test_events_generated_from_the_same_runtime_instance_should_have_the_same_generator_id():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.append_transaction(Decimal('5.00'))
    account.set_overdraft_limit(Decimal('10.00'))
    account.close()
    pending = account._collect_()
    assert pending[0].originator_id == pending[1].originator_id
    assert pending[0].originator_id == pending[2].originator_id
    assert pending[0].originator_id == pending[3].originator_id


def test_each_event_generated_from_the_same_runtime_instance_should_have_the_version_increased():
    account = BankAccount.open(full_name='Alice', email_address='alice@example.com')
    account.append_transaction(Decimal('5.00'))
    account.set_overdraft_limit(Decimal('10.00'))
    account.close()
    pending = account._collect_()
    assert pending[0].originator_version < pending[1].originator_version
    assert pending[1].originator_version < pending[2].originator_version
    assert pending[2].originator_version < pending[3].originator_version
