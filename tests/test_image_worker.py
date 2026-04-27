import asyncio
import io
import socket
import uuid

import httpx
import pytest
import pytest_asyncio
import uvicorn
import websockets
from fastapi.testclient import TestClient
from PIL import Image
from sqlalchemy import delete, select

import src.image_processing as image_processing
import src.main as main_module
import src.models as models
from src.broker import manager
from src.broker_protocol import (
    AckMessage,
    DeliverMessage,
    PublishMessage,
    SubscribeMessage,
    SubscribedMessage,
    decode_server_message,
    encode_wire_message,
)
from src.database import SessionLocal
from src.main import app
from worker import run_worker


def _make_png_bytes(color: tuple[int, int, int] = (25, 100, 200)) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (8, 8), color).save(output, format="PNG")
    return output.getvalue()


def _delete_user_data(user_id: str) -> None:
    with SessionLocal() as db:
        db.execute(delete(models.File).where(models.File.user_id == user_id))
        db.execute(delete(models.Bucket).where(models.Bucket.user_id == user_id))
        db.commit()


def _create_bucket_and_object(client, user_id: str, bucket_name: str) -> tuple[int, str, str]:
    create_bucket = client.post(
        "/buckets/",
        headers={"X-User-Id": user_id},
        json={"name": bucket_name},
    )
    assert create_bucket.status_code == 201

    bucket_id = create_bucket.json()["id"]
    upload = client.post(
        "/objects/upload",
        headers={"X-User-Id": user_id},
        data={"bucket_id": str(bucket_id)},
        files={"file": ("source.png", _make_png_bytes(), "image/png")},
    )
    assert upload.status_code == 201

    payload = upload.json()
    return bucket_id, payload["id"], payload["filename"]


def _send_client_message(websocket, message, message_format: str) -> None:
    wire_message = encode_wire_message(message, message_format)
    if isinstance(wire_message, bytes):
        websocket.send_bytes(wire_message)
    else:
        websocket.send_text(wire_message)


def _receive_server_message(websocket, message_format: str):
    if message_format == "msgpack":
        return decode_server_message(websocket.receive_bytes())
    return decode_server_message(websocket.receive_text())


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def _wait_for_server(base_url: str) -> None:
    deadline = asyncio.get_running_loop().time() + 10.0
    async with httpx.AsyncClient(base_url=base_url, timeout=1.0) as client:
        while True:
            try:
                response = await client.get("/openapi.json")
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass

            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("Timed out waiting for uvicorn test server.")
            await asyncio.sleep(0.05)


@pytest.fixture(autouse=True)
def cleanup_broker_state():
    with SessionLocal() as db:
        db.execute(delete(models.QueuedMessage))
        db.commit()

    manager.reset()
    yield

    with SessionLocal() as db:
        db.execute(delete(models.QueuedMessage))
        db.commit()

    manager.reset()


@pytest_asyncio.fixture
async def live_server(monkeypatch, tmp_path):
    monkeypatch.setattr(main_module, "STORAGE_DIR", tmp_path)
    main_module.STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    port = _get_free_port()
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        ws_ping_interval=600,
        ws_ping_timeout=600,
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    base_url = f"http://127.0.0.1:{port}"
    await _wait_for_server(base_url)

    try:
        yield {
            "http_base_url": base_url,
            "broker_url": f"ws://127.0.0.1:{port}/broker",
        }
    finally:
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=10.0)


def test_process_endpoint_enqueues_image_job(monkeypatch, tmp_path):
    user_id = f"process-{uuid.uuid4().hex}"
    bucket_name = f"bucket-{uuid.uuid4().hex[:20]}"
    monkeypatch.setattr(main_module, "STORAGE_DIR", tmp_path)
    main_module.STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    try:
        with TestClient(app) as client:
            bucket_id, object_id, filename = _create_bucket_and_object(
                client,
                user_id,
                bucket_name,
            )

            with client.websocket_connect("/broker?format=json") as subscriber:
                _send_client_message(
                    subscriber,
                    SubscribeMessage(
                        action="subscribe",
                        topic=image_processing.IMAGE_JOBS_TOPIC,
                    ),
                    "json",
                )
                subscribed = _receive_server_message(subscriber, "json")
                assert isinstance(subscribed, SubscribedMessage)

                response = client.post(
                    f"/buckets/{bucket_id}/objects/{object_id}/process",
                    headers={"X-User-Id": user_id},
                    json={"operation": "grayscale"},
                )

                assert response.status_code == 202
                body = response.json()
                assert body["status"] == "processing_started"
                assert body["bucket_id"] == bucket_id
                assert body["object_id"] == object_id
                assert body["topic"] == image_processing.IMAGE_JOBS_TOPIC

                delivered = _receive_server_message(subscriber, "json")
                assert isinstance(delivered, DeliverMessage)
                assert delivered.message_id == body["message_id"]
                assert delivered.topic == image_processing.IMAGE_JOBS_TOPIC
                assert delivered.payload == {
                    "source_bucket_id": bucket_id,
                    "source_object_id": object_id,
                    "source_filename": filename,
                    "user_id": user_id,
                    "request": {"operation": "grayscale"},
                }

                _send_client_message(
                    subscriber,
                    AckMessage(action="ack", message_id=delivered.message_id),
                    "json",
                )
    finally:
        _delete_user_data(user_id)


@pytest.mark.asyncio
async def test_worker_processes_ten_jobs_and_emits_ten_done_messages(live_server):
    user_id = f"worker-{uuid.uuid4().hex}"
    bucket_name = f"bucket-{uuid.uuid4().hex[:20]}"

    try:
        async with httpx.AsyncClient(
            base_url=live_server["http_base_url"],
            timeout=30.0,
        ) as client:
            create_bucket = await client.post(
                "/buckets/",
                headers={"X-User-Id": user_id},
                json={"name": bucket_name},
            )
            assert create_bucket.status_code == 201
            bucket_id = create_bucket.json()["id"]

            upload = await client.post(
                "/objects/upload",
                headers={"X-User-Id": user_id},
                data={"bucket_id": str(bucket_id)},
                files={"file": ("source.png", _make_png_bytes(), "image/png")},
            )
            assert upload.status_code == 201
            source_payload = upload.json()

        ready_event = asyncio.Event()
        worker_task = asyncio.create_task(
            run_worker(
                http_base_url=live_server["http_base_url"],
                broker_url=live_server["broker_url"],
                message_format="json",
                max_jobs=10,
                ready_event=ready_event,
            )
        )

        try:
            await asyncio.wait_for(ready_event.wait(), timeout=10.0)

            async with websockets.connect(
                f"{live_server['broker_url']}?format=json",
                max_size=None,
            ) as observer, websockets.connect(
                f"{live_server['broker_url']}?format=json",
                max_size=None,
            ) as publisher:
                await observer.send(
                    encode_wire_message(
                        SubscribeMessage(
                            action="subscribe",
                            topic=image_processing.IMAGE_DONE_TOPIC,
                        ),
                        "json",
                    )
                )
                subscribed = decode_server_message(await observer.recv())
                assert isinstance(subscribed, SubscribedMessage)

                operations = [
                    {"operation": "invert"},
                    {"operation": "mirror"},
                    {"operation": "crop", "top": 1, "left": 1, "width": 6, "height": 6},
                    {"operation": "brightness", "amount": 40},
                    {"operation": "grayscale"},
                ]

                for index in range(10):
                    await publisher.send(
                        encode_wire_message(
                            PublishMessage(
                                action="publish",
                                topic=image_processing.IMAGE_JOBS_TOPIC,
                                payload={
                                    "source_bucket_id": bucket_id,
                                    "source_object_id": source_payload["id"],
                                    "source_filename": source_payload["filename"],
                                    "user_id": user_id,
                                    "request": operations[index % len(operations)],
                                },
                            ),
                            "json",
                        )
                    )

                done_payloads = []
                while len(done_payloads) < 10:
                    server_message = decode_server_message(await observer.recv())
                    if not isinstance(server_message, DeliverMessage):
                        continue

                    done_payloads.append(server_message.payload)
                    await observer.send(
                        encode_wire_message(
                            AckMessage(
                                action="ack",
                                message_id=server_message.message_id,
                            ),
                            "json",
                        )
                    )

            processed_jobs = await asyncio.wait_for(worker_task, timeout=10.0)
            assert processed_jobs == 10
            assert len(done_payloads) == 10
            assert all(payload["status"] == "completed" for payload in done_payloads)

            with SessionLocal() as db:
                files = db.scalars(
                    select(models.File)
                    .where(models.File.user_id == user_id)
                    .order_by(models.File.created_at.asc())
                ).all()

            assert len(files) == 11
            assert sum(file.filename.endswith(".png") for file in files) == 11
        finally:
            if not worker_task.done():
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)
    finally:
        _delete_user_data(user_id)


@pytest.mark.asyncio
async def test_worker_reports_invalid_operation_without_crashing(live_server):
    ready_event = asyncio.Event()
    worker_task = asyncio.create_task(
        run_worker(
            http_base_url=live_server["http_base_url"],
            broker_url=live_server["broker_url"],
            message_format="json",
            max_jobs=1,
            ready_event=ready_event,
        )
    )

    try:
        await asyncio.wait_for(ready_event.wait(), timeout=10.0)

        async with websockets.connect(
            f"{live_server['broker_url']}?format=json",
            max_size=None,
        ) as observer, websockets.connect(
            f"{live_server['broker_url']}?format=json",
            max_size=None,
        ) as publisher:
            await observer.send(
                encode_wire_message(
                    SubscribeMessage(
                        action="subscribe",
                        topic=image_processing.IMAGE_DONE_TOPIC,
                    ),
                    "json",
                )
            )
            subscribed = decode_server_message(await observer.recv())
            assert isinstance(subscribed, SubscribedMessage)

            await publisher.send(
                encode_wire_message(
                    PublishMessage(
                        action="publish",
                        topic=image_processing.IMAGE_JOBS_TOPIC,
                        payload={
                            "source_bucket_id": 1,
                            "source_object_id": "missing",
                            "source_filename": "source.png",
                            "user_id": "anonymous",
                            "request": {"operation": "exploit-op"},
                        },
                    ),
                    "json",
                )
            )

            while True:
                server_message = decode_server_message(await observer.recv())
                if not isinstance(server_message, DeliverMessage):
                    continue

                await observer.send(
                    encode_wire_message(
                        AckMessage(action="ack", message_id=server_message.message_id),
                        "json",
                    )
                )
                assert server_message.payload["status"] == "failed"
                assert "Invalid image job payload" in server_message.payload["error"]
                break

        processed_jobs = await asyncio.wait_for(worker_task, timeout=10.0)
        assert processed_jobs == 1
    finally:
        if not worker_task.done():
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
