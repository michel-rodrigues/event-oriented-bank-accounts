from src.domain_model.mapper.cipher import SECRET_KEY, Cipher


def test_encrypt_and_decrypt_the_message():
    message = b'some message'
    cipher = Cipher(cipher_key=SECRET_KEY)
    encrypted_message = cipher.encrypt(message)
    assert cipher.decrypt(encrypted_message) == message
