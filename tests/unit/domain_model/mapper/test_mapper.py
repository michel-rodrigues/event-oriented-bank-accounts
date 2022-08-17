import uuid
import zlib
from datetime import datetime
from decimal import Decimal

from src.domain_model.aggregate import BankAccount
from src.domain_model.mapper.cipher import SECRET_KEY, Cipher
from src.patterns.domain_model_layer.mapper import Mapper, StoredEvent


def test_map_event_to_store_event(json_transcoder):
    event = BankAccount.TransactionAppended(
        originator_id=uuid.uuid4(), originator_version=1, timestamp=datetime.now(), amount=Decimal('10.00')
    )
    mapper = Mapper(transcoder=json_transcoder)
    stored_event = mapper.from_event(event)
    assert isinstance(stored_event, StoredEvent)


def test_map_store_event_to_event(json_transcoder):
    original_event = BankAccount.TransactionAppended(
        amount=Decimal('10.00'),
        originator_id=uuid.uuid4(),
        originator_version=1,
        timestamp=datetime.now(),
    )
    mapper = Mapper(transcoder=json_transcoder)
    stored_event = mapper.from_event(original_event)
    event: BankAccount.TransactionAppended = mapper.to_event(stored_event)
    assert event.amount == original_event.amount
    assert event.timestamp == original_event.timestamp
    assert event.originator_id == original_event.originator_id
    assert event.originator_version == original_event.originator_version


def test_when_cipher_is_provided_it_should_encrypt_the_state_on_store_event(json_transcoder):
    event = BankAccount.Opened(
        full_name='Steve Magal',
        email_address='steve.magal@irmaodojorel.com',
        originator_id=uuid.uuid4(),
        originator_version=1,
        originator_topic='dummy_value',
        timestamp=datetime.now(),
    )
    mapper = Mapper(cipher=Cipher(SECRET_KEY), transcoder=json_transcoder)
    stored_event = mapper.from_event(event)
    assert b'full_name' not in stored_event.state
    assert b'Steve Magal' not in stored_event.state
    assert b'email_address' not in stored_event.state
    assert b'steve.magal@irmaodojorel.com' not in stored_event.state


def test_when_cipher_is_provided_it_should_decrypt_the_state_on_store_event(json_transcoder):
    original_event = BankAccount.Opened(
        full_name='Steve Magal',
        email_address='steve.magal@irmaodojorel.com',
        originator_id=uuid.uuid4(),
        originator_version=1,
        originator_topic='dummy_value',
        timestamp=datetime.now(),
    )
    mapper = Mapper(cipher=Cipher(SECRET_KEY), transcoder=json_transcoder)
    stored_event = mapper.from_event(original_event)
    assert b'Steve Magal' not in stored_event.state
    event: BankAccount.Opened = mapper.to_event(stored_event)
    assert event.full_name == original_event.full_name
    assert event.email_address == original_event.email_address


def test_when_cipher_is_not_provided_it_should_not_encrypt_the_state_on_store_event(json_transcoder):
    event = BankAccount.Opened(
        full_name='Steve Magal',
        email_address='steve.magal@irmaodojorel.com',
        originator_id=uuid.uuid4(),
        originator_version=1,
        originator_topic='dummy_value',
        timestamp=datetime.now(),
    )
    mapper = Mapper(transcoder=json_transcoder)
    stored_event = mapper.from_event(event)
    assert b'full_name' in stored_event.state
    assert b'Steve Magal' in stored_event.state
    assert b'email_address' in stored_event.state
    assert b'steve.magal@irmaodojorel.com' in stored_event.state


def test_when_compressor_is_provided_it_should_compress_the_state_on_store_event(json_transcoder):
    event = BankAccount.Opened(
        full_name='Steve Magal',
        email_address='steve.magal@irmaodojorel.com',
        originator_id=uuid.uuid4(),
        originator_version=1,
        originator_topic='dummy_value',
        timestamp=datetime.now(),
    )
    mapper = Mapper(transcoder=json_transcoder)
    stored_event = mapper.from_event(event)

    mapper_with_compressor = Mapper(compressor=zlib, transcoder=json_transcoder)
    compressed_stored_event = mapper_with_compressor.from_event(event)

    assert len(compressed_stored_event.state) < len(stored_event.state)


def test_it_should_decompress_the_state_on_store_event(json_transcoder):
    original_event = BankAccount.Opened(
        full_name='Steve Magal',
        email_address='steve.magal@irmaodojorel.com',
        originator_id=uuid.uuid4(),
        originator_version=1,
        originator_topic='dummy_value',
        timestamp=datetime.now(),
    )
    mapper = Mapper(compressor=zlib, transcoder=json_transcoder)
    stored_event = mapper.from_event(original_event)
    event: BankAccount.Opened = mapper.to_event(stored_event)
    assert event.full_name == original_event.full_name
    assert event.email_address == original_event.email_address


def test_it_should_compress_and_encrypt_the_state_on_store_event(json_transcoder):
    original_event = BankAccount.Opened(
        full_name='Steve Magal',
        email_address='steve.magal@irmaodojorel.com',
        originator_id=uuid.uuid4(),
        originator_version=1,
        originator_topic='dummy_value',
        timestamp=datetime.now(),
    )
    mapper = Mapper(cipher=Cipher(SECRET_KEY), transcoder=json_transcoder)
    not_compressed_encrypted_stored_event = mapper.from_event(original_event)

    mapper = Mapper(cipher=Cipher(SECRET_KEY), compressor=zlib, transcoder=json_transcoder)
    compressed_encrypted_stored_event = mapper.from_event(original_event)

    assert b'Steve Magal' not in compressed_encrypted_stored_event.state
    assert len(compressed_encrypted_stored_event.state) < len(not_compressed_encrypted_stored_event.state)
