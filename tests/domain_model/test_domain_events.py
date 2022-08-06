import uuid
from dataclasses import FrozenInstanceError
from datetime import datetime

import pytest

from src.domain_model.domain_events import AccountClosed, AccountOpened, FullNameUpdated


ORIGINATOR_ID = uuid.uuid4()


def test_account_open_attributes():
    timestamp = datetime.now()
    event = AccountOpened(
        originator_id=ORIGINATOR_ID,
        originator_version=1,
        timestamp=timestamp,
        full_name='Alice',
    )
    assert event.originator_id == ORIGINATOR_ID
    assert event.originator_version == 1
    assert event.timestamp == timestamp
    assert event.full_name == 'Alice'


def test_it_should_not_possible_to_change_account_open_attributes():
    event = AccountOpened(
        originator_id=ORIGINATOR_ID,
        originator_version=1,
        timestamp=datetime.now(),
        full_name='Alice',
    )
    with pytest.raises(FrozenInstanceError):
        event.originator_id = uuid.uuid4()


def test_full_name_updated_attributes():
    timestamp = datetime.now()
    event = FullNameUpdated(
        originator_id=ORIGINATOR_ID,
        originator_version=2,
        timestamp=timestamp,
        full_name='Bob',
    )
    assert event.originator_id == ORIGINATOR_ID
    assert event.originator_version == 2
    assert event.timestamp == timestamp
    assert event.full_name == 'Bob'


def test_it_should_not_possible_to_change_full_name_updated_attributes():
    event = FullNameUpdated(
        originator_id=ORIGINATOR_ID,
        originator_version=2,
        timestamp=datetime.now(),
        full_name='Alice',
    )
    with pytest.raises(FrozenInstanceError):
        event.originator_id = uuid.uuid4()


def test_account_closed_attributes():
    timestamp = datetime.now()
    event = AccountClosed(
        originator_id=ORIGINATOR_ID,
        originator_version=3,
        timestamp=timestamp,
    )
    assert event.originator_id == ORIGINATOR_ID
    assert event.originator_version == 3
    assert event.timestamp == timestamp


def test_it_should_not_possible_to_change_account_closed_attributes():
    event = AccountClosed(
        originator_id=ORIGINATOR_ID,
        originator_version=3,
        timestamp=datetime.now(),
    )
    with pytest.raises(FrozenInstanceError):
        event.originator_id = uuid.uuid4()
