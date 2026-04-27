import asyncio
import json
from typing import Any

import msgpack
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError
from sqlalchemy import select

import src.models as models
from src.broker_protocol import (
    AckMessage,
    DeliverMessage,
    ErrorMessage,
    MessageFormat,
    PublishMessage,
    SubscribeMessage,
    SubscribedMessage,
    decode_wire_message,
    encode_wire_message,
    normalize_message_format,
)
from src.database import SessionLocal

router = APIRouter()


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: dict[str, set[WebSocket]] = {}
        self.connection_topics: dict[WebSocket, set[str]] = {}
        self.connection_formats: dict[WebSocket, MessageFormat] = {}
        self.send_locks: dict[WebSocket, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def connect(
        self, websocket: WebSocket, message_format: MessageFormat
    ) -> None:
        await websocket.accept()
        async with self._lock:
            self.connection_topics[websocket] = set()
            self.connection_formats[websocket] = message_format
            self.send_locks[websocket] = asyncio.Lock()

    async def subscribe(self, websocket: WebSocket, topic: str) -> bool:
        async with self._lock:
            topics = self.connection_topics.setdefault(websocket, set())
            if topic in topics:
                return False

            topics.add(topic)
            self.active_connections.setdefault(topic, set()).add(websocket)
            return True

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            topics = self.connection_topics.pop(websocket, set())
            self.connection_formats.pop(websocket, None)
            self.send_locks.pop(websocket, None)

            for topic in topics:
                subscribers = self.active_connections.get(topic)
                if subscribers is None:
                    continue

                subscribers.discard(websocket)
                if not subscribers:
                    del self.active_connections[topic]

    async def send_message(
        self,
        websocket: WebSocket,
        message: SubscribedMessage | DeliverMessage | ErrorMessage,
    ) -> None:
        async with self._lock:
            message_format = self.connection_formats.get(websocket)
            send_lock = self.send_locks.get(websocket)

        if message_format is None or send_lock is None:
            return

        wire_message = encode_wire_message(message, message_format)

        try:
            # Multiple publishers can target the same subscriber concurrently, so
            # each socket gets its own send lock to keep frame ordering stable.
            async with send_lock:
                if isinstance(wire_message, bytes):
                    await websocket.send_bytes(wire_message)
                else:
                    await websocket.send_text(wire_message)
        except Exception:
            await self.disconnect(websocket)

    async def broadcast(self, topic: str, message: DeliverMessage) -> None:
        async with self._lock:
            subscribers = list(self.active_connections.get(topic, set()))

        if not subscribers:
            return

        await asyncio.gather(
            *(self.send_message(websocket, message) for websocket in subscribers)
        )

    def topic_subscriber_count(self, topic: str) -> int:
        return len(self.active_connections.get(topic, set()))

    def reset(self) -> None:
        self.active_connections.clear()
        self.connection_topics.clear()
        self.connection_formats.clear()
        self.send_locks.clear()


manager = ConnectionManager()


def _store_queued_message(topic: str, payload: Any) -> int:
    try:
        serialized_payload = json.dumps(payload)
    except TypeError as exc:
        raise ValueError("Payload must be JSON serializable for persistence.") from exc

    with SessionLocal() as db:
        queued_message = models.QueuedMessage(topic=topic, payload=serialized_payload)
        db.add(queued_message)
        db.commit()
        db.refresh(queued_message)
        return queued_message.id


async def publish_persistent_message(topic: str, payload: Any) -> int:
    message_id = await run_in_threadpool(_store_queued_message, topic, payload)
    await manager.broadcast(
        topic,
        DeliverMessage(topic=topic, message_id=message_id, payload=payload),
    )
    return message_id


def _load_pending_messages(topic: str) -> list[dict[str, Any]]:
    with SessionLocal() as db:
        queued_messages = db.scalars(
            select(models.QueuedMessage)
            .where(
                models.QueuedMessage.topic == topic,
                models.QueuedMessage.is_delivered.is_(False),
            )
            .order_by(models.QueuedMessage.id.asc())
        ).all()

        return [
            {
                "topic": queued_message.topic,
                "message_id": queued_message.id,
                "payload": json.loads(queued_message.payload),
            }
            for queued_message in queued_messages
        ]


def _acknowledge_message(message_id: int) -> bool:
    with SessionLocal() as db:
        queued_message = db.get(models.QueuedMessage, message_id)
        if queued_message is None:
            return False

        if not queued_message.is_delivered:
            queued_message.is_delivered = True
            db.commit()

        return True


async def _receive_wire_message(websocket: WebSocket) -> str | bytes:
    message = await websocket.receive()
    if message["type"] == "websocket.disconnect":
        raise WebSocketDisconnect(code=message.get("code", 1000))

    if message.get("bytes") is not None:
        return message["bytes"]

    if message.get("text") is not None:
        return message["text"]

    raise ValueError("Unsupported WebSocket frame.")


@router.websocket("/broker")
async def websocket_broker(websocket: WebSocket) -> None:
    try:
        message_format = normalize_message_format(websocket.query_params.get("format"))
    except ValueError:
        await websocket.close(code=1003, reason="Unsupported message format")
        return

    await manager.connect(websocket, message_format)

    try:
        while True:
            try:
                inbound_message = decode_wire_message(
                    await _receive_wire_message(websocket)
                )
            except (
                ValidationError,
                ValueError,
                TypeError,
                msgpack.exceptions.ExtraData,
                msgpack.exceptions.FormatError,
                msgpack.exceptions.StackError,
            ) as exc:
                await manager.send_message(
                    websocket,
                    ErrorMessage(detail=f"Invalid broker message: {exc}"),
                )
                continue

            if isinstance(inbound_message, SubscribeMessage):
                is_new_subscription = await manager.subscribe(
                    websocket, inbound_message.topic
                )
                await manager.send_message(
                    websocket,
                    SubscribedMessage(topic=inbound_message.topic),
                )

                if is_new_subscription:
                    pending_messages = await run_in_threadpool(
                        _load_pending_messages, inbound_message.topic
                    )
                    for pending_message in pending_messages:
                        await manager.send_message(
                            websocket,
                            DeliverMessage(**pending_message),
                        )
                continue

            if isinstance(inbound_message, PublishMessage):
                try:
                    await publish_persistent_message(
                        inbound_message.topic,
                        inbound_message.payload,
                    )
                except ValueError as exc:
                    await manager.send_message(websocket, ErrorMessage(detail=str(exc)))
                    continue
                continue

            if isinstance(inbound_message, AckMessage):
                acknowledged = await run_in_threadpool(
                    _acknowledge_message, inbound_message.message_id
                )
                if not acknowledged:
                    await manager.send_message(
                        websocket,
                        ErrorMessage(
                            detail=f"Message {inbound_message.message_id} was not found."
                        ),
                    )
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(websocket)
