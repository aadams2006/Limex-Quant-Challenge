"""Credential loading utilities for Lime trading scripts.

This module centralizes how sensitive configuration such as API keys are
retrieved.  Credentials are primarily loaded from environment variables and
optionally fall back to a local JSON file (which should never be committed to
source control).  This helps keep secrets out of the repository while
maintaining backwards compatibility for local development setups.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


@dataclass
class Credentials:
    """Container for the authentication details required by the Lime APIs."""

    client_id: str
    client_secret: str
    username: str
    password: str
    auth_url: str
    base_url: str
    account_number: Optional[str] = None

    @property
    def resolved_account_number(self) -> str:
        """Return the provided account number or derive a demo account fallback.

        Some historical scripts inferred a demo account number from the username
        by stripping the email domain and appending ``"@demo"``.  To preserve
        this behaviour while allowing explicit overrides, this property prefers
        the supplied ``account_number`` field and only computes a fallback when
        necessary.
        """

        if self.account_number:
            return self.account_number
        if "@" not in self.username:
            raise ValueError(
                "Unable to derive account number from username without '@'. "
                "Set LIME_ACCOUNT_NUMBER or add 'account_number' to your credentials file."
            )
        return f"{self.username.split('@')[0]}@demo"


_ENV_MAP: Dict[str, str] = {
    "client_id": "LIME_CLIENT_ID",
    "client_secret": "LIME_CLIENT_SECRET",
    "username": "LIME_USERNAME",
    "password": "LIME_PASSWORD",
    "auth_url": "LIME_AUTH_URL",
    "base_url": "LIME_BASE_URL",
    "account_number": "LIME_ACCOUNT_NUMBER",
}

_REQUIRED_FIELDS = {"client_id", "client_secret", "username", "password", "auth_url", "base_url"}


def _load_from_env() -> Dict[str, str]:
    """Retrieve any credential values defined in environment variables."""

    data: Dict[str, str] = {}
    for field, env_var in _ENV_MAP.items():
        value = os.getenv(env_var)
        if value:
            data[field] = value
    return data


def _load_from_file(path: Path) -> Dict[str, str]:
    """Read credential values from ``path`` when it exists."""

    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        loaded = json.load(handle)
    # Normalise keys to lower snake_case for consistency
    return {key.lower(): value for key, value in loaded.items() if value}


def get_credentials() -> Credentials:
    """Load credentials from the environment and optional JSON configuration.

    Values are read from environment variables first.  Any missing fields are
    then populated from the JSON file specified by the ``LIME_CREDENTIALS_FILE``
    environment variable (defaulting to ``credentials.json``).  If required
    values remain missing after both sources are considered, a ``RuntimeError``
    is raised with guidance on how to provide them.
    """

    credentials: Dict[str, str] = _load_from_env()

    credentials_file = Path(os.getenv("LIME_CREDENTIALS_FILE", "credentials.json"))
    file_data = _load_from_file(credentials_file)

    for key, value in file_data.items():
        credentials.setdefault(key, value)

    missing = sorted(field for field in _REQUIRED_FIELDS if field not in credentials)
    if missing:
        env_list = ", ".join(_ENV_MAP[field] for field in missing)
        raise RuntimeError(
            "Missing required Lime API credentials. Set the environment "
            f"variables ({env_list}) or provide them in {credentials_file}."
        )

    return Credentials(**credentials)


__all__ = ["Credentials", "get_credentials"]
