from cryptography.fernet import Fernet

from src.patterns.domain_model_layer.mapper import AbstractCipher


# This must be kept secret.
# It was generated with Fernet.generate_key()
SECRET_KEY = b'S53I-eu9U9PJ3O3OS6KiT4l-ICBlPbWqIzHFY18jNjA='


class Cipher(AbstractCipher):
    """Cipher strategy that uses cryptography library Fernet cipher."""

    def __init__(self, cipher_key: bytes):
        self._cipher = Fernet(cipher_key)

    def encrypt(self, plaintext: bytes) -> bytes:
        """Return ciphertext for given plaintext."""
        return self._cipher.encrypt(plaintext)

    def decrypt(self, ciphertext: bytes) -> bytes:
        return self._cipher.decrypt(ciphertext)
