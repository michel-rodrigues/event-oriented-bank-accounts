import uuid

from src.domain_model.notification.postgresql import PostgresApplicationRecorder
from src.patterns.domain_model.mapper import StoredEvent


def test_it_should_write_the_stored_events():
    recorder = PostgresApplicationRecorder()
    recorder.create_table()

    originator_id_1 = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id_1, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id_1, originator_version=2, topic='topic2', state=b'state2')

    originator_id_2 = uuid.uuid4()
    stored_event3 = StoredEvent(originator_id=originator_id_2, originator_version=3, topic='topic3', state=b'state3')

    recorder.insert_events([stored_event1, stored_event2])
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


def test_max_notification_id_should_be_the_total_number_of_the_stored_events():
    recorder = PostgresApplicationRecorder()
    recorder.create_table()

    originator_id_1 = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id_1, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id_1, originator_version=2, topic='topic2', state=b'state2')

    originator_id_2 = uuid.uuid4()
    stored_event3 = StoredEvent(originator_id=originator_id_2, originator_version=3, topic='topic3', state=b'state3')

    recorder.insert_events([stored_event1, stored_event2])
    recorder.insert_events([stored_event3])

    assert recorder.max_notification_id() == 3


def test_max_notification_id_should_be_return_zero_when_there_is_no_stored_events():
    recorder = PostgresApplicationRecorder()
    recorder.create_table()
    assert recorder.max_notification_id() == 0


def test_it_should_select_notifications_which_the_id_attribute_are_greater_than_the_provided_value():
    recorder = PostgresApplicationRecorder()
    recorder.create_table()

    originator_id_1 = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id_1, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id_1, originator_version=2, topic='topic2', state=b'state2')

    originator_id_2 = uuid.uuid4()
    stored_event3 = StoredEvent(originator_id=originator_id_2, originator_version=3, topic='topic3', state=b'state3')

    recorder.insert_events([stored_event1, stored_event2])
    recorder.insert_events([stored_event3])

    notifications = recorder.select_notifications(start=2, limit=10)
    assert len(notifications) == 2
    assert notifications[0].originator_id == stored_event2.originator_id
    assert notifications[0].originator_version == stored_event2.originator_version
    assert notifications[0].topic == stored_event2.topic
    assert notifications[0].state == stored_event2.state
    assert notifications[1].originator_id == stored_event3.originator_id
    assert notifications[1].originator_version == stored_event3.originator_version
    assert notifications[1].topic == stored_event3.topic
    assert notifications[1].state == stored_event3.state


def test_the_quantity_of_selected_notifications_should_be_limited_by_the_provided_value():
    recorder = PostgresApplicationRecorder()
    recorder.create_table()

    originator_id_1 = uuid.uuid4()
    stored_event1 = StoredEvent(originator_id=originator_id_1, originator_version=1, topic='topic1', state=b'state1')
    stored_event2 = StoredEvent(originator_id=originator_id_1, originator_version=2, topic='topic2', state=b'state2')
    stored_event3 = StoredEvent(originator_id=originator_id_1, originator_version=3, topic='topic3', state=b'state3')

    originator_id_2 = uuid.uuid4()
    stored_event4 = StoredEvent(originator_id=originator_id_2, originator_version=4, topic='topic4', state=b'state4')
    stored_event5 = StoredEvent(originator_id=originator_id_2, originator_version=5, topic='topic5', state=b'state5')
    stored_event6 = StoredEvent(originator_id=originator_id_2, originator_version=6, topic='topic6', state=b'state6')
    stored_event7 = StoredEvent(originator_id=originator_id_2, originator_version=7, topic='topic7', state=b'state7')

    recorder.insert_events([stored_event1, stored_event2, stored_event3])
    recorder.insert_events([stored_event4, stored_event5, stored_event6, stored_event7])

    notifications = recorder.select_notifications(start=3, limit=4)
    assert len(notifications) == 4
    assert notifications[0].originator_id == stored_event3.originator_id
    assert notifications[0].originator_version == stored_event3.originator_version
    assert notifications[0].topic == stored_event3.topic
    assert notifications[0].state == stored_event3.state
    assert notifications[1].originator_id == stored_event4.originator_id
    assert notifications[1].originator_version == stored_event4.originator_version
    assert notifications[1].topic == stored_event4.topic
    assert notifications[1].state == stored_event4.state
    assert notifications[2].originator_id == stored_event5.originator_id
    assert notifications[2].originator_version == stored_event5.originator_version
    assert notifications[2].topic == stored_event5.topic
    assert notifications[2].state == stored_event5.state
    assert notifications[3].originator_id == stored_event6.originator_id
    assert notifications[3].originator_version == stored_event6.originator_version
    assert notifications[3].topic == stored_event6.topic
    assert notifications[3].state == stored_event6.state
