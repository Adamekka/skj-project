import asyncio
import socket
import uuid

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

import src.haystack as haystack_module
import src.main as main_module
import src.models as models
from src.database import SessionLocal
from src.main import app as gateway_app


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


def test_haystack_reads_exact_bytes_and_rotates_volumes(monkeypatch, tmp_path):
    monkeypatch.setattr(haystack_module, "VOLUME_DIR", tmp_path)
    monkeypatch.setattr(haystack_module, "MAX_VOLUME_BYTES", 5)
    monkeypatch.setattr(haystack_module, "HAYSTACK_BROKER_LISTENER_ENABLED", False)

    with TestClient(haystack_module.app) as client:
        store = haystack_module.app.state.volume_store

        async def append_payloads():
            first_location = await store.append(b"abc")
            second_location = await store.append(b"def")
            return first_location, second_location

        first, second = asyncio.run(append_payloads())

        assert first == {"volume_id": 1, "offset": 0, "size": 3}
        assert second == {"volume_id": 2, "offset": 0, "size": 3}

        first_read = client.get("/volume/1/0/3")
        second_read = client.get("/volume/2/0/3")

    assert first_read.status_code == 200
    assert first_read.content == b"abc"
    assert second_read.status_code == 200
    assert second_read.content == b"def"


def test_gateway_lists_and_updates_volume_locations():
    user_id = f"compact-{uuid.uuid4().hex}"
    bucket_name = f"compact-{uuid.uuid4().hex[:20]}"
    object_id = str(uuid.uuid4())
    deleted_object_id = str(uuid.uuid4())

    try:
        with SessionLocal() as db:
            bucket = models.Bucket(name=bucket_name, user_id=user_id)
            db.add(bucket)
            db.commit()
            db.refresh(bucket)

            db.add_all(
                [
                    models.File(
                        id=object_id,
                        user_id=user_id,
                        bucket_id=bucket.id,
                        filename="live.jpg",
                        path="haystack://volume/1/10/4",
                        size=4,
                        status="ready",
                        volume_id=1,
                        offset=10,
                    ),
                    models.File(
                        id=deleted_object_id,
                        user_id=user_id,
                        bucket_id=bucket.id,
                        filename="deleted.jpg",
                        path="haystack://volume/1/0/7",
                        size=7,
                        status="ready",
                        volume_id=1,
                        offset=0,
                        is_deleted=True,
                    ),
                ]
            )
            db.commit()

        with TestClient(gateway_app) as client:
            listed = client.get("/admin/volumes/1/objects")
            assert listed.status_code == 200
            assert listed.json() == [
                {"object_id": object_id, "volume_id": 1, "offset": 10, "size": 4}
            ]

            updated = client.patch(
                f"/admin/objects/{object_id}/location",
                json={"volume_id": 1, "offset": 0, "size": 4},
            )
            assert updated.status_code == 200
            assert updated.json() == {
                "object_id": object_id,
                "volume_id": 1,
                "offset": 0,
                "size": 4,
                "status": "ready",
            }

        with SessionLocal() as db:
            record = db.scalar(select(models.File).where(models.File.id == object_id))
            assert record is not None
            assert record.offset == 0
            assert record.path == "haystack://volume/1/0/4"
    finally:
        with SessionLocal() as db:
            db.execute(delete(models.File).where(models.File.user_id == user_id))
            db.execute(delete(models.Bucket).where(models.Bucket.user_id == user_id))
            db.commit()


@pytest.mark.asyncio
async def test_haystack_compact_endpoint_rewrites_volume_and_updates_gateway(
    monkeypatch,
    tmp_path,
):
    user_id = f"compact-post-{uuid.uuid4().hex}"
    bucket_name = f"compact-{uuid.uuid4().hex[:20]}"
    first_object_id = str(uuid.uuid4())
    second_object_id = str(uuid.uuid4())
    deleted_object_id = str(uuid.uuid4())

    monkeypatch.setattr(haystack_module, "VOLUME_DIR", tmp_path)
    monkeypatch.setattr(haystack_module, "HAYSTACK_BROKER_LISTENER_ENABLED", False)
    monkeypatch.setattr(main_module, "STORAGE_ACK_LISTENER_ENABLED", False)

    gateway_port = _get_free_port()
    gateway_base_url = f"http://127.0.0.1:{gateway_port}"
    monkeypatch.setattr(haystack_module, "S3_GATEWAY_URL", gateway_base_url)

    config = uvicorn.Config(
        gateway_app,
        host="127.0.0.1",
        port=gateway_port,
        log_level="warning",
        ws_ping_interval=600,
        ws_ping_timeout=600,
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await _wait_for_server(gateway_base_url)
    haystack_server = None
    haystack_task = None

    try:
        with SessionLocal() as db:
            bucket = models.Bucket(name=bucket_name, user_id=user_id)
            db.add(bucket)
            db.commit()
            db.refresh(bucket)

            db.add_all(
                [
                    models.File(
                        id=deleted_object_id,
                        user_id=user_id,
                        bucket_id=bucket.id,
                        filename="deleted.jpg",
                        path="haystack://volume/1/0/3",
                        size=3,
                        status="ready",
                        volume_id=1,
                        offset=0,
                        is_deleted=True,
                    ),
                    models.File(
                        id=first_object_id,
                        user_id=user_id,
                        bucket_id=bucket.id,
                        filename="first.jpg",
                        path="haystack://volume/1/3/3",
                        size=3,
                        status="ready",
                        volume_id=1,
                        offset=3,
                    ),
                    models.File(
                        id=second_object_id,
                        user_id=user_id,
                        bucket_id=bucket.id,
                        filename="second.jpg",
                        path="haystack://volume/1/9/3",
                        size=3,
                        status="ready",
                        volume_id=1,
                        offset=9,
                    ),
                ]
            )
            db.commit()

        volume_path = tmp_path / "volume_1.dat"
        volume_path.write_bytes(b"delonegaptwo")
        original_volume_size = volume_path.stat().st_size
        haystack_port = _get_free_port()
        haystack_base_url = f"http://127.0.0.1:{haystack_port}"
        haystack_config = uvicorn.Config(
            haystack_module.app,
            host="127.0.0.1",
            port=haystack_port,
            log_level="warning",
            ws_ping_interval=600,
            ws_ping_timeout=600,
        )
        haystack_server = uvicorn.Server(haystack_config)
        haystack_task = asyncio.create_task(haystack_server.serve())
        await _wait_for_server(haystack_base_url)

        async with httpx.AsyncClient(base_url=haystack_base_url, timeout=30.0) as client:
            response = await client.post("/admin/volumes/1/compact")

        assert response.status_code == 200
        assert response.json() == {
            "volume_id": 1,
            "moved_objects": 2,
            "bytes_written": 6,
        }
        assert volume_path.read_bytes() == b"onetwo"
        assert volume_path.stat().st_size == 6
        assert volume_path.stat().st_size < original_volume_size

        with SessionLocal() as db:
            first_record = db.get(models.File, first_object_id)
            second_record = db.get(models.File, second_object_id)
            deleted_record = db.get(models.File, deleted_object_id)

            assert first_record is not None
            assert first_record.offset == 0
            assert first_record.path == "haystack://volume/1/0/3"
            assert second_record is not None
            assert second_record.offset == 3
            assert second_record.path == "haystack://volume/1/3/3"
            assert deleted_record is not None
            assert deleted_record.offset == 0
    finally:
        if haystack_server is not None and haystack_task is not None:
            haystack_server.should_exit = True
            await asyncio.wait_for(haystack_task, timeout=10.0)
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=10.0)
        with SessionLocal() as db:
            db.execute(delete(models.File).where(models.File.user_id == user_id))
            db.execute(delete(models.Bucket).where(models.Bucket.user_id == user_id))
            db.commit()
