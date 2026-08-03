"""Secrets vault.

Stores secrets encrypted at rest using Fernet (AES-128-CBC + HMAC). The master
key comes from ``PHIF_VAULT_MASTER_KEY``. The vault interface is intentionally
small so an external backend (e.g. HashiCorp Vault) can be slotted in later.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
from typing import Any, Protocol

from cryptography.fernet import Fernet, InvalidToken

from phif.config import get_settings

log = logging.getLogger(__name__)


def _derive_dev_key() -> str:
    """Deterministic, INSECURE key for local dev when none is configured."""
    digest = hashlib.sha256(b"phif-insecure-dev-key").digest()
    return base64.urlsafe_b64encode(digest).decode()


class SecretsVault(Protocol):
    def encrypt(self, plaintext: dict[str, Any]) -> str: ...
    def decrypt(self, token: str) -> dict[str, Any]: ...


class FernetVault:
    """Default vault: symmetric encryption of JSON secret bundles."""

    def __init__(self, master_key: str | None = None):
        key = master_key or get_settings().vault_master_key
        if not key:
            log.warning(
                "PHIF_VAULT_MASTER_KEY is not set — using an INSECURE dev key. "
                "Set a real Fernet key in production."
            )
            key = _derive_dev_key()
        try:
            self._fernet = Fernet(key.encode() if isinstance(key, str) else key)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                "PHIF_VAULT_MASTER_KEY must be a urlsafe-base64 32-byte Fernet key. "
                "Generate one with: python -c \"from cryptography.fernet import Fernet; "
                'print(Fernet.generate_key().decode())"'
            ) from exc

    def encrypt(self, plaintext: dict[str, Any]) -> str:
        raw = json.dumps(plaintext, separators=(",", ":")).encode()
        return self._fernet.encrypt(raw).decode()

    def decrypt(self, token: str) -> dict[str, Any]:
        try:
            raw = self._fernet.decrypt(token.encode())
        except InvalidToken as exc:
            raise ValueError("Failed to decrypt secret (wrong master key or corrupt data)") from exc
        return json.loads(raw)

    @staticmethod
    def generate_key() -> str:
        return Fernet.generate_key().decode()


_vault: FernetVault | None = None


def get_vault() -> FernetVault:
    global _vault
    if _vault is None:
        _vault = FernetVault()
    return _vault
