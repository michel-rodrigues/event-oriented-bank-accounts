import uuid

from src.application.notification_log import LocalNotificationLog
from src.domain_model.notification.in_memory import InMemoryApplicationRecorder
from src.patterns.domain_model.mapper import StoredEvent


def test_it_should_return_an_empty_section_when_there_is_no_stored_events():
    recorder = InMemoryApplicationRecorder()
    notification_log = LocalNotificationLog(recorder, section_size=5)
    section = notification_log['1,5']
    assert len(section.items) == 0
    assert section.section_id is None
    assert section.next_id is None


def test_provided_section_size_should_limit_the_number_of_notifications_on_the_section():
    SECTION_SIZE = 5
    recorder = InMemoryApplicationRecorder()
    notification_log = LocalNotificationLog(recorder, section_size=SECTION_SIZE)
    originator_id = uuid.uuid4()
    recorder.insert_events(
        [
            StoredEvent(
                originator_id=originator_id,
                originator_version=i,
                topic='topic',
                state=b'state',
            )
            for i in range(6)
        ]
    )
    section = notification_log['1,6']
    assert len(section.items) == SECTION_SIZE
    assert section.section_id == f'1,{SECTION_SIZE}'
    assert section.next_id == '6,10'


def test_next_id_should_be_none_when_there_are_less_stored_events_then_size_of_the_section():
    recorder = InMemoryApplicationRecorder()
    notification_log = LocalNotificationLog(recorder, section_size=5)
    originator_id = uuid.uuid4()
    recorder.insert_events(
        [
            StoredEvent(
                originator_id=originator_id,
                originator_version=i,
                topic='topic',
                state=b'state',
            )
            for i in range(4)
        ]
    )
    section = notification_log['1,5']
    assert len(section.items) == 4
    assert section.section_id == '1,4'
    assert section.next_id is None


def test_it_should_select_the_second_section():
    recorder = InMemoryApplicationRecorder()
    notification_log = LocalNotificationLog(recorder, section_size=5)
    originator_id = uuid.uuid4()
    recorder.insert_events(
        [
            StoredEvent(
                originator_id=originator_id,
                originator_version=i,
                topic='topic',
                state=b'state',
            )
            for i in range(5)
        ]
    )
    originator_id = uuid.uuid4()
    recorder.insert_events(
        [
            StoredEvent(
                originator_id=originator_id,
                originator_version=i,
                topic='topic',
                state=b'state',
            )
            for i in range(4)
        ]
    )
    section = notification_log['6,10']
    assert len(section.items) == 4
    assert section.section_id == '6,9'
    assert section.next_id is None
