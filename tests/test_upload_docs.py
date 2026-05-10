import json
import uuid

from fastapi.testclient import TestClient
from sqlalchemy import delete, select

import src.main as main_module
import src.models as models
from src.broker_protocol import decode_binary_values
from src.database import SessionLocal
from src.main import app
from src.storage_protocol import STORAGE_WRITE_TOPIC


def test_upload_openapi_uses_generated_multipart_schema():
    app.openapi_schema = None
    with TestClient(app) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 200

    openapi = response.json()
    assert openapi["openapi"] == "3.0.3"

    request_body = openapi["paths"]["/objects/upload"]["post"]["requestBody"]
    schema = request_body["content"]["multipart/form-data"]["schema"]

    assert schema == {"$ref": "#/components/schemas/Body_upload_object_objects_upload_post"}

    component = openapi["components"]["schemas"]["Body_upload_object_objects_upload_post"]
    assert component["required"] == ["bucket_id", "file"]
    assert component["properties"]["bucket_id"]["type"] == "integer"
    assert component["properties"]["bucket_id"]["minimum"] == 1
    assert component["properties"]["file"]["type"] == "string"
    assert component["properties"]["file"]["format"] == "binary"
    assert "contentMediaType" not in component["properties"]["file"]


def test_openapi_shows_download_paths_without_legacy_duplicates():
    app.openapi_schema = None
    with TestClient(app) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 200

    paths = response.json()["paths"]
    assert "/download/{object_id}" in paths
    assert "get" in paths["/download/{object_id}"]
    assert "delete" in paths["/download/{object_id}"]
    assert "/objects/{object_id}" not in paths


def test_openapi_shows_gateway_compact_endpoint():
    app.openapi_schema = None
    with TestClient(app) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 200

    paths = response.json()["paths"]
    assert "/admin/volumes/{volume_id}/compact" in paths
    assert "post" in paths["/admin/volumes/{volume_id}/compact"]


def test_upload_object_accepts_multipart_file(monkeypatch, tmp_path):
    user_id = f"upload-docs-{uuid.uuid4().hex}"
    bucket_name = f"upload-{uuid.uuid4().hex[:20]}"
    monkeypatch.setattr(main_module, "STORAGE_DIR", tmp_path)

    try:
        with TestClient(app) as client:
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
                files={"file": ("notes.txt", b"hello upload", "text/plain")},
            )

        assert upload.status_code == 202
        payload = upload.json()
        assert payload["bucket_id"] == bucket_id
        assert payload["filename"] == "notes.txt"
        assert payload["size"] == 12
        assert payload["status"] == "uploading"

        with SessionLocal() as db:
            record = db.scalar(select(models.File).where(models.File.id == payload["id"]))
            assert record is not None
            assert record.path == f"haystack://pending/{payload['id']}"
            assert record.status == "uploading"
            assert record.volume_id is None
            assert record.offset is None

            queued_payload = None
            queued_messages = db.scalars(
                select(models.QueuedMessage).where(
                    models.QueuedMessage.topic == STORAGE_WRITE_TOPIC
                )
            ).all()
            for queued_message in queued_messages:
                persisted_payload = json.loads(queued_message.payload)
                candidate_payload = decode_binary_values(persisted_payload["payload"])
                if candidate_payload["object_id"] == payload["id"]:
                    queued_payload = candidate_payload
                    break

            assert queued_payload == {
                "object_id": payload["id"],
                "data": b"hello upload",
            }
    finally:
        with SessionLocal() as db:
            db.execute(delete(models.File).where(models.File.user_id == user_id))
            db.execute(delete(models.Bucket).where(models.Bucket.user_id == user_id))
            db.execute(delete(models.QueuedMessage))
            db.commit()


def test_upload_object_rejects_bucket_id_zero():
    with TestClient(app) as client:
        response = client.post(
            "/objects/upload",
            data={"bucket_id": "0"},
            files={"file": ("notes.txt", b"hello upload", "text/plain")},
        )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "bucket_id"]
