from __future__ import annotations

import base64
import hashlib
from cryptography.fernet import Fernet


class SecretBox:
    def __init__(self, master_key: str) -> None:
        digest = hashlib.sha256(master_key.encode("utf-8")).digest()
        key = base64.urlsafe_b64encode(digest)
        self._fernet = Fernet(key)

    def encrypt(self, value: str) -> str:
        return self._fernet.encrypt(value.encode("utf-8")).decode("utf-8")

    def decrypt(self, value: str) -> str:
        return self._fernet.decrypt(value.encode("utf-8")).decode("utf-8")
