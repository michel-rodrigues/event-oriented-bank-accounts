import uuid

import pytest

from src.domain_model.recorders import SQLiteAggregateRecorder
from src.patterns.domain_model.mapper import StoredEvent
from src.patterns.domain_model.recorder import Recorder


def test_record_events_and_retrieve_ordered_by_version():
    originator_id = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id, originator_version=2, topic='topic2', state=b'state2')

    recorder = SQLiteAggregateRecorder()
    recorder.create_table()
    recorder.insert_events([stored_event1, stored_event2])
    results = recorder.select_events(originator_id)

    assert len(results) == 2
    assert results[0].originator_id == stored_event1.originator_id
    assert results[0].originator_version == stored_event1.originator_version
    assert results[0].topic == stored_event1.topic
    assert results[0].state == stored_event1.state
    assert results[1].originator_id == stored_event2.originator_id
    assert results[1].originator_version == stored_event2.originator_version
    assert results[1].topic == stored_event2.topic
    assert results[1].state == stored_event2.state


def test_it_should_not_allow_overwrite_an_stored_event():
    originator_id = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id, originator_version=2, topic='topic2', state=b'state2')

    recorder = SQLiteAggregateRecorder()
    recorder.create_table()
    recorder.insert_events([stored_event1, stored_event2])

    with pytest.raises(Recorder.IntegrityError):
        recorder.insert_events([stored_event1])


def test_insert_events_should_be_an_atomic_operation():
    originator_id = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id, originator_version=2, topic='topic2', state=b'state2')

    recorder = SQLiteAggregateRecorder()
    recorder.create_table()
    recorder.insert_events([stored_event1, stored_event2])

    stored_event3 = StoredEvent(originator_id=originator_id, originator_version=3, topic='topic3', state=b'state3')

    with pytest.raises(Recorder.IntegrityError):
        recorder.insert_events([stored_event3, stored_event2])

    results = recorder.select_events(originator_id)
    assert len(results) == 2
    assert results[0].originator_id == stored_event1.originator_id
    assert results[0].originator_version == stored_event1.originator_version
    assert results[0].topic == stored_event1.topic
    assert results[0].state == stored_event1.state
    assert results[1].originator_id == stored_event2.originator_id
    assert results[1].originator_version == stored_event2.originator_version
    assert results[1].topic == stored_event2.topic
    assert results[1].state == stored_event2.state


def test_select_stored_events_by_originator_id():
    recorder = SQLiteAggregateRecorder()
    recorder.create_table()

    originator_id_1 = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id_1, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id_1, originator_version=2, topic='topic2', state=b'state2')
    recorder.insert_events([stored_event1, stored_event2])

    originator_id_2 = uuid.uuid4()
    stored_event3 = StoredEvent(originator_id=originator_id_2, originator_version=3, topic='topic3', state=b'state3')
    recorder.insert_events([stored_event3])

    results = recorder.select_events(originator_id_1)
    assert len(results) == 2
    assert results[0].originator_id == stored_event1.originator_id
    assert results[0].originator_version == stored_event1.originator_version
    assert results[0].topic == stored_event1.topic
    assert results[0].state == stored_event1.state
    assert results[1].originator_id == stored_event2.originator_id
    assert results[1].originator_version == stored_event2.originator_version
    assert results[1].topic == stored_event2.topic
    assert results[1].state == stored_event2.state

    results = recorder.select_events(originator_id_2)
    assert len(results) == 1
    assert results[0].originator_id == stored_event3.originator_id
    assert results[0].originator_version == stored_event3.originator_version
    assert results[0].topic == stored_event3.topic
    assert results[0].state == stored_event3.state


def test_it_should_select_stored_events_greater_than_the_provided_version():
    originator_id = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id, originator_version=2, topic='topic2', state=b'state2')
    stored_event3 = StoredEvent(originator_id=originator_id, originator_version=3, topic='topic3', state=b'state3')

    recorder = SQLiteAggregateRecorder()
    recorder.create_table()
    recorder.insert_events([stored_event1, stored_event2, stored_event3])
    results = recorder.select_events(originator_id, gt=1)
    assert len(results) == 2
    assert results[0].originator_id == stored_event2.originator_id
    assert results[0].originator_version == stored_event2.originator_version
    assert results[1].originator_id == stored_event3.originator_id
    assert results[1].originator_version == stored_event3.originator_version


def test_it_should_select_stored_events_less_than_and_equal_the_provided_version():
    originator_id = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id, originator_version=2, topic='topic2', state=b'state2')
    stored_event3 = StoredEvent(originator_id=originator_id, originator_version=3, topic='topic3', state=b'state3')

    recorder = SQLiteAggregateRecorder()
    recorder.create_table()
    recorder.insert_events([stored_event1, stored_event2, stored_event3])
    results = recorder.select_events(originator_id, lte=2)
    assert len(results) == 2
    assert results[0].originator_id == stored_event1.originator_id
    assert results[0].originator_version == stored_event1.originator_version
    assert results[1].originator_id == stored_event2.originator_id
    assert results[1].originator_version == stored_event2.originator_version


def test_it_should_select_stored_events_between_the_provided_version():
    originator_id = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id, originator_version=2, topic='topic2', state=b'state2')
    stored_event3 = StoredEvent(originator_id=originator_id, originator_version=3, topic='topic3', state=b'state3')

    recorder = SQLiteAggregateRecorder()
    recorder.create_table()
    recorder.insert_events([stored_event1, stored_event2, stored_event3])
    results = recorder.select_events(originator_id, gt=1, lte=2)
    assert len(results) == 1
    assert results[0].originator_id == stored_event2.originator_id
    assert results[0].originator_version == stored_event2.originator_version


def test_it_should_limit_the_amount_of_selected_stored_events_equal_to_the_provided_value():
    originator_id = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id, originator_version=2, topic='topic2', state=b'state2')
    stored_event3 = StoredEvent(originator_id=originator_id, originator_version=3, topic='topic3', state=b'state3')

    recorder = SQLiteAggregateRecorder()
    recorder.create_table()
    recorder.insert_events([stored_event1, stored_event2, stored_event3])
    results = recorder.select_events(originator_id, limit=2)
    assert len(results) == 2
    assert results[0].originator_id == stored_event1.originator_id
    assert results[0].originator_version == stored_event1.originator_version
    assert results[1].originator_id == stored_event2.originator_id
    assert results[1].originator_version == stored_event2.originator_version


def test_it_should_return_stored_events_sorted_by_version_descending():
    originator_id = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id, originator_version=2, topic='topic2', state=b'state2')
    stored_event3 = StoredEvent(originator_id=originator_id, originator_version=3, topic='topic3', state=b'state3')

    recorder = SQLiteAggregateRecorder()
    recorder.create_table()
    recorder.insert_events([stored_event1, stored_event2, stored_event3])
    results = recorder.select_events(originator_id, desc=True)
    assert len(results) == 3
    assert results[0].originator_id == stored_event3.originator_id
    assert results[0].originator_version == stored_event3.originator_version
    assert results[1].originator_id == stored_event2.originator_id
    assert results[1].originator_version == stored_event2.originator_version
    assert results[2].originator_id == stored_event1.originator_id
    assert results[2].originator_version == stored_event1.originator_version


def test_it_should_be_possible_mix_all_select_events_parameters():
    originator_id = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id, originator_version=2, topic='topic2', state=b'state2')
    stored_event3 = StoredEvent(originator_id=originator_id, originator_version=3, topic='topic3', state=b'state3')
    stored_event4 = StoredEvent(originator_id=originator_id, originator_version=4, topic='topic4', state=b'state4')
    stored_event5 = StoredEvent(originator_id=originator_id, originator_version=5, topic='topic5', state=b'state5')

    recorder = SQLiteAggregateRecorder()
    recorder.create_table()
    recorder.insert_events([stored_event1, stored_event2, stored_event3, stored_event4, stored_event5])
    results = recorder.select_events(originator_id, gt=1, lte=4, limit=2, desc=True)
    assert len(results) == 2
    assert results[0].originator_id == stored_event4.originator_id
    assert results[0].originator_version == stored_event4.originator_version
    assert results[1].originator_id == stored_event3.originator_id
    assert results[1].originator_version == stored_event3.originator_version
