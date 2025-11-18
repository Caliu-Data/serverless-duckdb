from __future__ import annotations

import re
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from rich.console import Console

console = Console()

_PLACEHOLDER_PATTERN = re.compile(r"\{\{\s*keyvault:([a-zA-Z0-9\-_]+)\s*\}\}")
_ENV_PATTERN = re.compile(r"\{\{\s*env:([A-Z0-9_]+)\s*\}\}")


@dataclass
class KeyVaultConfig:
    vault_url: str


class SecretResolver:
    def __init__(self, config: KeyVaultConfig):
        self.config = config
        credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=config.vault_url, credential=credential)
        self._cache: Dict[str, str] = {}

    def resolve_structure(self, data: Any, skip_keys: Optional[Iterable[str]] = None) -> Any:
        if skip_keys is None:
            skip_keys = ()

        if isinstance(data, dict):
            return {
                key: self.resolve_structure(value, skip_keys)
                if key not in skip_keys
                else value
                for key, value in data.items()
            }
        if isinstance(data, list):
            return [self.resolve_structure(item, skip_keys) for item in data]
        if isinstance(data, str):
            return self._replace_placeholders(data)
        return data

    def _replace_placeholders(self, value: str) -> str:
        def _replace_env(match: re.Match[str]) -> str:
            env_var = match.group(1)
            if env_var not in os.environ:
                raise ValueError(f"Environment variable '{env_var}' is not set but is required by the configuration")
            return os.environ[env_var]

        value = _ENV_PATTERN.sub(_replace_env, value)

        def _lookup(match: re.Match[str]) -> str:
            secret_name = match.group(1)
            return self._get_secret(secret_name)

        if _PLACEHOLDER_PATTERN.search(value):
            console.log(f"[magenta]Resolving secrets in configuration value[/]")
            return _PLACEHOLDER_PATTERN.sub(_lookup, value)
        return value

    def _get_secret(self, secret_name: str) -> str:
        if secret_name in self._cache:
            return self._cache[secret_name]
        try:
            secret = self.client.get_secret(secret_name)
        except ResourceNotFoundError as exc:
            raise ValueError(f"Secret '{secret_name}' not found in Key Vault {self.config.vault_url}") from exc
        self._cache[secret_name] = secret.value
        return secret.value

