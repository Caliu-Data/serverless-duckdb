from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from adlfs import AzureBlobFileSystem
from azure.identity import DefaultAzureCredential
from rich.console import Console

console = Console()


@dataclass
class ADLSClient:
    account_name: str
    file_system: str
    credential: Optional[str] = None

    def _fs(self) -> AzureBlobFileSystem:
        if self.credential:
            token = self.credential
        else:
            token = DefaultAzureCredential()
        return AzureBlobFileSystem(account_name=self.account_name, credential=token)

    def upload(self, local_path: Path, remote_path: str) -> str:
        fs = self._fs()
        console.log(f"[green]Uploading {local_path} to abfs://{self.file_system}/{remote_path}[/]")
        with local_path.open("rb") as data:
            fs.upload(
                data,
                f"{self.file_system}/{remote_path}",
                overwrite=True,
            )
        return f"abfs://{self.file_system}/{remote_path}"

