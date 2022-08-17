import uuid
from decimal import Decimal

from src.domain_model.aggregate import BankAccount
from src.patterns.application_layer.application.application import Application


class BankAccounts(Application):
    def open_account(self, full_name, email_address) -> uuid.UUID:
        account = BankAccount.open(full_name=full_name, email_address=email_address)
        self.save(account)
        return account.id

    def credit_account(self, account_id: uuid.UUID, amount: Decimal):
        account = self.get_account(account_id)
        account.append_transaction(amount)
        self.save(account)

    def get_balance(self, account_id: uuid.UUID) -> Decimal:
        account = self.get_account(account_id)
        return account.balance

    def get_account(self, account_id: uuid.UUID) -> BankAccount:
        try:
            aggregate = self.repository.get(account_id)
        except self.repository.AggregateNotFoundError:
            raise self.AccountNotFoundError(account_id)
        else:
            if not isinstance(aggregate, BankAccount):
                raise self.AccountNotFoundError(account_id)
            return aggregate

    class AccountNotFoundError(Exception):
        pass
