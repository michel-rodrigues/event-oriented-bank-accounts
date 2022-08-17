from decimal import Decimal

from src.application.application import BankAccounts


def test_notiﬁcation_log_should_be_initially_empty():
    app = BankAccounts()
    section = app.log['1,10']
    assert len(section.items) == 0


def test_it_should_created_an_account():
    app = BankAccounts()
    account_id = app.open_account(full_name='Alice', email_address='alice@example.com')
    account = app.get_account(account_id)
    assert account.id == account_id


def test_it_should_get_balance_from_an_account_id():
    app = BankAccounts()
    account_id = app.open_account(full_name='Alice', email_address='alice@example.com')
    app.credit_account(account_id, Decimal('10.00'))
    app.credit_account(account_id, Decimal('25.00'))
    app.credit_account(account_id, Decimal('30.00'))
    assert app.get_balance(account_id) == Decimal('65.00')


def test_it_should_have_events_notiﬁcations_in_the_notiﬁcation_log():
    app = BankAccounts()
    account_id = app.open_account(full_name='Alice', email_address='alice@example.com')
    app.credit_account(account_id, Decimal('10.00'))
    app.credit_account(account_id, Decimal('25.00'))
    app.credit_account(account_id, Decimal('30.00'))
    section = app.log['1,10']
    assert len(section.items) == 4
