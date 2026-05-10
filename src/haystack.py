import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, BinaryIO

import httpx
import websockets
from fastapi import FastAPI, HTTPException, Path as PathParam
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from pydantic import BaseModel, Field

from src.broker_protocol import (
    AckMessage,
    DeliverMessage,
    ErrorMessage,
    MessageFormat,
    PublishMessage,
    SubscribeMessage,
    SubscribedMessage,
    decode_server_message,
    encode_wire_message,
)
from src.storage_protocol import STORAGE_ACK_TOPIC, STORAGE_WRITE_TOPIC, websocket_url

DEFAULT_VOLUME_DIR = Path(__file__).resolve().parent.parent / "haystack_volumes"
VOLUME_DIR = Path(os.getenv("HAYSTACK_VOLUME_DIR", str(DEFAULT_VOLUME_DIR)))
MAX_VOLUME_BYTES = int(os.getenv("HAYSTACK_MAX_VOLUME_BYTES", str(100 * 1024 * 1024)))
HAYSTACK_BROKER_URL = os.getenv("HAYSTACK_BROKER_URL", "ws://127.0.0.1:8000/broker")
HAYSTACK_BROKER_LISTENER_ENABLED = os.getenv("HAYSTACK_BROKER_LISTENER_ENABLED", "1") != "0"
S3_GATEWAY_URL = os.getenv("S3_GATEWAY_URL", "http://127.0.0.1:8000")
BROKER_RECONNECT_DELAY_SECONDS = 0.2


class CompactVolumeResponse(BaseModel):
    volume_id: int = Field(..., ge=1)
    moved_objects: int = Field(..., ge=0)
    bytes_written: int = Field(..., ge=0)


class VolumeStore:
    def __init__(self, volume_dir: Path, max_volume_bytes: int) -> None:
        self.volume_dir = volume_dir
        self.max_volume_bytes = max_volume_bytes
        self.active_volume_id = 1
        self.active_file: BinaryIO | None = None
        self._lock = asyncio.Lock()

    def open(self) -> None:
        self.volume_dir.mkdir(parents=True, exist_ok=True)
        self.active_volume_id = self._latest_volume_id()
        self.active_file = open(self._volume_path(self.active_volume_id), "ab+")
        self.active_file.seek(0, os.SEEK_END)

    def close(self) -> None:
        if self.active_file is None:
            return
        self.active_file.close()
        self.active_file = None

    async def append(self, data: bytes) -> dict[str, int]:
        async with self._lock:
            return await run_in_threadpool(self._append_locked, data)

    async def read(self, volume_id: int, offset: int, size: int) -> bytes:
        async with self._lock:
            return await run_in_threadpool(self._read_bytes, volume_id, offset, size)

    async def compact(self, volume_id: int, gateway_url: str) -> dict[str, int]:
        async with self._lock:
            if self.active_file is None:
                raise RuntimeError("Volume store is not open.")

            was_active_volume = volume_id == self.active_volume_id
            if was_active_volume:
                self.active_file.flush()
                self.active_file.close()
                self.active_file = None

            try:
                async with httpx.AsyncClient(base_url=gateway_url, timeout=30.0) as client:
                    response = await client.get(f"/admin/volumes/{volume_id}/objects")
                    response.raise_for_status()
                    records: list[dict[str, Any]] = response.json()

                    updates = await run_in_threadpool(
                        self._rewrite_compacted_volume,
                        volume_id,
                        records,
                    )
                    for update in updates:
                        location_response = await client.patch(
                            f"/admin/objects/{update['object_id']}/location",
                            json={
                                "volume_id": volume_id,
                                "offset": update["offset"],
                                "size": update["size"],
                            },
                        )
                        location_response.raise_for_status()
            finally:
                if was_active_volume:
                    self.active_file = open(self._volume_path(self.active_volume_id), "ab+")
                    self.active_file.seek(0, os.SEEK_END)

            return {
                "volume_id": volume_id,
                "moved_objects": len(updates),
                "bytes_written": sum(update["size"] for update in updates),
            }

    def _latest_volume_id(self) -> int:
        volume_ids: list[int] = []
        for path in self.volume_dir.glob("volume_*.dat"):
            suffix = path.stem.removeprefix("volume_")
            if suffix.isdecimal():
                volume_ids.append(int(suffix))
        return max(volume_ids, default=1)

    def _volume_path(self, volume_id: int) -> Path:
        return self.volume_dir / f"volume_{volume_id}.dat"

    def _append_locked(self, data: bytes) -> dict[str, int]:
        if self.active_file is None:
            raise RuntimeError("Volume store is not open.")

        self.active_file.seek(0, os.SEEK_END)
        offset = self.active_file.tell()
        if offset > 0 and offset + len(data) > self.max_volume_bytes:
            self.active_file.close()
            self.active_volume_id += 1
            self.active_file = open(self._volume_path(self.active_volume_id), "ab+")
            self.active_file.seek(0, os.SEEK_END)
            offset = self.active_file.tell()

        self.active_file.write(data)
        self.active_file.flush()
        return {
            "volume_id": self.active_volume_id,
            "offset": offset,
            "size": len(data),
        }

    def _read_bytes(self, volume_id: int, offset: int, size: int) -> bytes:
        volume_path = self._volume_path(volume_id)
        if not volume_path.exists():
            raise FileNotFoundError(volume_path)

        with open(volume_path, "rb") as input_file:
            input_file.seek(offset)
            data = input_file.read(size)

        if len(data) != size:
            raise ValueError("Requested volume range is not available.")
        return data

    def _rewrite_compacted_volume(
        self,
        volume_id: int,
        records: list[dict[str, Any]],
    ) -> list[dict[str, int | str]]:
        source_path = self._volume_path(volume_id)
        compacted_path = self.volume_dir / f"volume_{volume_id}_compacted.dat"
        if not source_path.exists():
            raise FileNotFoundError(source_path)

        updates: list[dict[str, int | str]] = []
        with open(source_path, "rb") as source_file:
            with open(compacted_path, "wb") as compacted_file:
                for record in records:
                    source_file.seek(record["offset"])
                    data = source_file.read(record["size"])
                    if len(data) != record["size"]:
                        raise ValueError(
                            f"Object {record['object_id']} is missing bytes in volume {volume_id}."
                        )

                    new_offset = compacted_file.tell()
                    compacted_file.write(data)
                    updates.append(
                        {
                            "object_id": record["object_id"],
                            "offset": new_offset,
                            "size": record["size"],
                        }
                    )

        compacted_path.replace(source_path)
        return updates


async def _send_broker_message(
    websocket: websockets.ClientConnection,
    message: SubscribeMessage | PublishMessage | AckMessage,
    message_format: MessageFormat,
) -> None:
    await websocket.send(encode_wire_message(message, message_format))


async def _storage_write_listener(store: VolumeStore) -> None:
    message_format: MessageFormat = "msgpack"
    broker_url = websocket_url(HAYSTACK_BROKER_URL, message_format)

    while True:
        try:
            async with websockets.connect(broker_url, max_size=None) as websocket:
                await _send_broker_message(
                    websocket,
                    SubscribeMessage(action="subscribe", topic=STORAGE_WRITE_TOPIC),
                    message_format,
                )

                while True:
                    server_message = decode_server_message(await websocket.recv())
                    if isinstance(server_message, SubscribedMessage):
                        continue
                    if isinstance(server_message, ErrorMessage):
                        continue
                    if not isinstance(server_message, DeliverMessage):
                        continue

                    payload = server_message.payload
                    if not isinstance(payload, dict):
                        await _send_broker_message(
                            websocket,
                            AckMessage(action="ack", message_id=server_message.message_id),
                            message_format,
                        )
                        continue

                    object_id = payload.get("object_id")
                    data = payload.get("data")
                    if not isinstance(object_id, str) or not isinstance(data, bytes):
                        await _send_broker_message(
                            websocket,
                            AckMessage(action="ack", message_id=server_message.message_id),
                            message_format,
                        )
                        continue

                    location = await store.append(data)
                    await _send_broker_message(
                        websocket,
                        PublishMessage(
                            action="publish",
                            topic=STORAGE_ACK_TOPIC,
                            payload={"object_id": object_id, **location},
                        ),
                        message_format,
                    )
                    await _send_broker_message(
                        websocket,
                        AckMessage(action="ack", message_id=server_message.message_id),
                        message_format,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(BROKER_RECONNECT_DELAY_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = VolumeStore(VOLUME_DIR, MAX_VOLUME_BYTES)
    await run_in_threadpool(store.open)
    app.state.volume_store = store
    storage_write_task: asyncio.Task | None = None
    if HAYSTACK_BROKER_LISTENER_ENABLED:
        storage_write_task = asyncio.create_task(_storage_write_listener(store))
        app.state.storage_write_task = storage_write_task

    try:
        yield
    finally:
        if storage_write_task is not None:
            storage_write_task.cancel()
            await asyncio.gather(storage_write_task, return_exceptions=True)
        await run_in_threadpool(store.close)


app = FastAPI(
    title="Haystack Storage Node",
    description="Append-only binary volume storage node.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/volume/{volume_id}/{offset}/{size}")
async def read_volume(
    volume_id: int = PathParam(..., ge=1),
    offset: int = PathParam(..., ge=0),
    size: int = PathParam(..., ge=0),
) -> Response:
    store: VolumeStore | None = getattr(app.state, "volume_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Storage node is not ready")

    try:
        data = await store.read(volume_id, offset, size)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Volume not found")
    except ValueError:
        raise HTTPException(status_code=416, detail="Requested range is unavailable")

    return Response(content=data, media_type="image/jpeg")


@app.post(
    "/admin/volumes/{volume_id}/compact",
    response_model=CompactVolumeResponse,
    tags=["admin"],
    summary="Compact a Haystack volume",
)
async def compact_volume(
    volume_id: int = PathParam(..., ge=1),
) -> CompactVolumeResponse:
    store: VolumeStore | None = getattr(app.state, "volume_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Storage node is not ready")

    try:
        result = await store.compact(volume_id, S3_GATEWAY_URL)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Volume not found")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Gateway compaction request failed: {exc}")

    return CompactVolumeResponse(**result)
