from __future__ import annotations

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from azure.storage.queue import QueueClient
from rich.console import Console

console = Console()


@dataclass
class QueueMessage:
    payload: Dict[str, Any]
    message_id: str
    pop_receipt: str


@dataclass
class AzureTaskQueue:
    queue_client: QueueClient
    visibility_timeout: int = 300

    @classmethod
    def from_connection_string(
        cls,
        connection_string: str,
        queue_name: str,
        visibility_timeout: int = 300,
    ) -> "AzureTaskQueue":
        client = QueueClient.from_connection_string(connection_string, queue_name)
        client.create_queue()
        console.log(f"[green]Using Azure Storage Queue '{queue_name}'[/]")
        return cls(queue_client=client, visibility_timeout=visibility_timeout)

    def purge(self) -> None:
        self.queue_client.clear_messages()

    def enqueue(self, payload: Dict[str, Any]) -> None:
        message = json.dumps(payload)
        self.queue_client.send_message(message)
        console.log(f"[cyan]Queued task: {payload}[/]")

    def receive(self) -> Optional[QueueMessage]:
        messages = self.queue_client.receive_messages(
            messages_per_page=1, visibility_timeout=self.visibility_timeout
        )
        try:
            message = next(iter(messages))
        except StopIteration:
            return None
        payload = json.loads(message.content)
        return QueueMessage(payload=payload, message_id=message.id, pop_receipt=message.pop_receipt)

    def delete(self, message: QueueMessage) -> None:
        self.queue_client.delete_message(message.message_id, message.pop_receipt)

    def is_empty(self) -> bool:
        peeked = self.queue_client.peek_messages(max_messages=1)
        try:
            next(iter(peeked))
        except StopIteration:
            return True
        return False

