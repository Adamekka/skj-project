import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import websockets
from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    Header,
    HTTPException,
    Path as PathParam,
    UploadFile,
)
from fastapi.concurrency import run_in_threadpool
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse, Response
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from src.broker import (
    create_queued_message,
    manager,
    publish_persistent_message,
    router as broker_router,
)
from src.broker_protocol import (
    AckMessage,
    DeliverMessage,
    ErrorMessage,
    MessageFormat,
    SubscribeMessage,
    SubscribedMessage,
    decode_server_message,
    encode_wire_message,
)
import src.image_processing as image_processing
import src.models as models
import src.schemas as schemas
from src.database import SessionLocal, get_db
from src.storage_protocol import (
    STORAGE_ACK_TOPIC,
    STORAGE_WRITE_TOPIC,
    haystack_location_path,
    websocket_url,
)

STORAGE_DIR = Path(__file__).resolve().parent.parent / "storage"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)
HAYSTACK_BASE_URL = os.getenv("HAYSTACK_BASE_URL", "http://127.0.0.1:8001")
GATEWAY_BROKER_URL = os.getenv("GATEWAY_BROKER_URL", "ws://127.0.0.1:8000/broker")
STORAGE_ACK_LISTENER_ENABLED = os.getenv("STORAGE_ACK_LISTENER_ENABLED", "1") != "0"
STORAGE_ACK_RECONNECT_DELAY_SECONDS = 0.2


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage_ack_task: asyncio.Task | None = None
    if STORAGE_ACK_LISTENER_ENABLED:
        storage_ack_task = asyncio.create_task(_storage_ack_listener())
        app.state.storage_ack_task = storage_ack_task

    try:
        yield
    finally:
        if storage_ack_task is not None:
            storage_ack_task.cancel()
            await asyncio.gather(storage_ack_task, return_exceptions=True)


app = FastAPI(
    title="Object Storage Service",
    description="Simple S3-inspired file storage backend with buckets and billing.",
    version="2.0.0",
    lifespan=lifespan,
)
app.openapi_version = "3.0.3"

app.include_router(broker_router)


def custom_openapi() -> dict[str, Any]:
    if app.openapi_schema is not None:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        routes=app.routes,
    )

    for path_item in openapi_schema.get("paths", {}).values():
        for operation in path_item.values():
            if not isinstance(operation, dict):
                continue
            multipart_schema = (
                operation.get("requestBody", {})
                .get("content", {})
                .get("multipart/form-data", {})
                .get("schema", {})
            )
            schema_ref = multipart_schema.get("$ref")
            if not schema_ref:
                continue

            schema_name = schema_ref.removeprefix("#/components/schemas/")
            component_schema = (
                openapi_schema.get("components", {})
                .get("schemas", {})
                .get(schema_name)
            )
            if component_schema is None:
                continue

            for property_schema in component_schema.get("properties", {}).values():
                if property_schema.get("type") != "string":
                    continue
                if "contentMediaType" not in property_schema:
                    continue

                # Swagger UI renders file pickers for `format: binary`, while the
                # current FastAPI/Pydantic stack emits `contentMediaType` here.
                property_schema.pop("contentMediaType", None)
                property_schema["format"] = "binary"

    app.openapi_schema = openapi_schema
    return openapi_schema


app.openapi = custom_openapi


def _get_bucket_or_404(bucket_id: int, user_id: str, db: Session) -> models.Bucket:
    bucket = db.scalar(
        select(models.Bucket).where(
            models.Bucket.id == bucket_id,
            models.Bucket.user_id == user_id,
        )
    )
    if bucket is None:
        raise HTTPException(status_code=404, detail="Bucket not found")
    return bucket


def _get_object_or_404(object_id: str, user_id: str, db: Session) -> models.File:
    record = db.scalar(
        select(models.File)
        .options(joinedload(models.File.bucket))
        .where(
            models.File.id == object_id,
            models.File.user_id == user_id,
            models.File.is_deleted.is_(False),
        )
    )
    if record is None:
        raise HTTPException(status_code=404, detail="Object not found")
    return record


def _apply_storage_ack(payload: dict[str, Any]) -> None:
    object_id = payload.get("object_id")
    volume_id = payload.get("volume_id")
    offset = payload.get("offset")
    size = payload.get("size")
    if not isinstance(object_id, str):
        return
    if (
        not isinstance(volume_id, int)
        or not isinstance(offset, int)
        or not isinstance(size, int)
    ):
        return

    with SessionLocal() as db:
        result = db.execute(
            update(models.File)
            .where(models.File.id == object_id, models.File.status != "ready")
            .values(
                volume_id=volume_id,
                offset=offset,
                size=size,
                status="ready",
                path=haystack_location_path(volume_id, offset, size),
            )
        )
        if result.rowcount != 1:
            record = db.get(models.File, object_id)
            if record is not None:
                record.volume_id = volume_id
                record.offset = offset
                record.size = size
                record.path = haystack_location_path(volume_id, offset, size)
                db.commit()
            return

        record = db.get(models.File, object_id)
        if record is None:
            db.commit()
            return

        record.bucket.current_storage_bytes += size
        record.bucket.bandwidth_bytes += size
        if record.uploaded_internally:
            record.bucket.internal_transfer_bytes += size
        else:
            record.bucket.ingress_bytes += size

        db.commit()


async def _send_broker_message(
    websocket: websockets.ClientConnection,
    message: SubscribeMessage | AckMessage,
    message_format: MessageFormat,
) -> None:
    await websocket.send(encode_wire_message(message, message_format))


async def _storage_ack_listener() -> None:
    message_format: MessageFormat = "msgpack"
    broker_url = websocket_url(GATEWAY_BROKER_URL, message_format)

    while True:
        try:
            async with websockets.connect(broker_url, max_size=None) as websocket:
                await _send_broker_message(
                    websocket,
                    SubscribeMessage(action="subscribe", topic=STORAGE_ACK_TOPIC),
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
                    if isinstance(server_message.payload, dict):
                        await run_in_threadpool(_apply_storage_ack, server_message.payload)
                    await _send_broker_message(
                        websocket,
                        AckMessage(action="ack", message_id=server_message.message_id),
                        message_format,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(STORAGE_ACK_RECONNECT_DELAY_SECONDS)


@app.post(
    "/buckets/",
    response_model=schemas.BucketRecord,
    status_code=201,
    tags=["buckets"],
    summary="Create a bucket",
    response_description="Metadata of the newly created bucket",
)
def create_bucket(
    bucket: schemas.BucketCreate,
    x_user_id: str = Header(default="anonymous", min_length=1),
    db: Session = Depends(get_db),
):
    record = models.Bucket(name=bucket.name, user_id=x_user_id)
    db.add(record)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Bucket name already exists")

    db.refresh(record)
    return record


@app.get(
    "/objects/",
    response_model=list[schemas.ObjectRecord],
    tags=["objects"],
    summary="List objects",
    response_description="Non-deleted objects owned by the requesting user across all buckets",
)
@app.get(
    "/files",
    include_in_schema=False,
    response_model=list[schemas.ObjectRecord],
)
def list_objects(
    x_user_id: str = Header(default="anonymous", min_length=1),
    db: Session = Depends(get_db),
):
    return db.scalars(
        select(models.File)
        .where(
            models.File.user_id == x_user_id,
            models.File.is_deleted.is_(False),
        )
        .order_by(models.File.created_at.desc())
    ).all()


@app.get(
    "/buckets/{bucket_id}/objects/",
    response_model=list[schemas.ObjectRecord],
    tags=["buckets"],
    summary="List objects in a bucket",
    response_description="Non-deleted objects stored in the selected bucket",
)
def list_bucket_objects(
    bucket_id: int = PathParam(..., ge=1),
    x_user_id: str = Header(default="anonymous", min_length=1),
    db: Session = Depends(get_db),
):
    _get_bucket_or_404(bucket_id, x_user_id, db)
    return db.scalars(
        select(models.File)
        .where(
            models.File.bucket_id == bucket_id,
            models.File.user_id == x_user_id,
            models.File.is_deleted.is_(False),
        )
        .order_by(models.File.created_at.desc())
    ).all()


@app.post(
    "/objects/upload",
    response_model=schemas.ObjectUploadResponse,
    status_code=202,
    tags=["objects"],
    summary="Upload an object",
    response_description="Metadata of the accepted object upload",
)
@app.post(
    "/files/upload",
    include_in_schema=False,
    response_model=schemas.ObjectUploadResponse,
    status_code=202,
)
async def upload_object(
    bucket_id: int = Form(..., ge=1),
    file: UploadFile = File(...),
    x_user_id: str = Header(default="anonymous", min_length=1),
    x_internal_source: bool = Header(default=False),
    db: Session = Depends(get_db),
):
    bucket = _get_bucket_or_404(bucket_id, x_user_id, db)
    object_id = str(uuid.uuid4())
    content = await file.read()

    record = models.File(
        id=object_id,
        user_id=x_user_id,
        bucket_id=bucket.id,
        filename=file.filename or object_id,
        path=f"haystack://pending/{object_id}",
        size=len(content),
        status="uploading",
        uploaded_internally=x_internal_source,
    )

    storage_payload = {
        "object_id": record.id,
        "data": content,
    }
    db.add(record)
    queued_message = create_queued_message(db, STORAGE_WRITE_TOPIC, storage_payload)
    message_id = queued_message.id

    try:
        db.commit()
    except Exception:
        db.rollback()
        raise

    await manager.broadcast(
        STORAGE_WRITE_TOPIC,
        DeliverMessage(
            topic=STORAGE_WRITE_TOPIC,
            message_id=message_id,
            payload=storage_payload,
        ),
    )

    return schemas.ObjectUploadResponse(
        id=record.id,
        bucket_id=record.bucket_id,
        filename=record.filename,
        size=record.size,
        status="uploading",
    )


@app.post(
    "/buckets/{bucket_id}/objects/{object_id}/process",
    response_model=image_processing.ProcessObjectResponse,
    status_code=202,
    tags=["objects"],
    summary="Start image processing",
    response_description="Acknowledgement that image processing was enqueued",
)
async def process_object(
    request: image_processing.ImageProcessRequest,
    bucket_id: int = PathParam(..., ge=1),
    object_id: str = PathParam(...),
    x_user_id: str = Header(default="anonymous", min_length=1),
    db: Session = Depends(get_db),
):
    bucket = _get_bucket_or_404(bucket_id, x_user_id, db)
    record = _get_object_or_404(object_id, x_user_id, db)
    if record.bucket_id != bucket.id:
        raise HTTPException(status_code=404, detail="Object not found in bucket")

    if record.status != "ready":
        raise HTTPException(status_code=409, detail="Object is still uploading")
    if record.volume_id is None and not Path(record.path).exists():
        raise HTTPException(status_code=500, detail="Object data missing from disk")

    message_id = await publish_persistent_message(
        image_processing.IMAGE_JOBS_TOPIC,
        image_processing.ImageProcessJob(
            source_bucket_id=bucket.id,
            source_object_id=record.id,
            source_filename=record.filename,
            user_id=x_user_id,
            request=request,
        ).model_dump(mode="json"),
    )
    return image_processing.ProcessObjectResponse(
        bucket_id=bucket.id,
        object_id=record.id,
        message_id=message_id,
    )


@app.get(
    "/objects/{object_id}",
    include_in_schema=False,
    tags=["objects"],
    summary="Download an object",
    response_description="Raw object bytes as an octet-stream",
)
@app.get(
    "/download/{object_id}",
    tags=["objects"],
    summary="Download an object",
    response_description="Raw object bytes as an octet-stream",
)
@app.get("/files/{object_id}", include_in_schema=False)
async def download_object(
    object_id: str,
    x_user_id: str = Header(default="anonymous", min_length=1),
    x_internal_source: bool = Header(default=False),
    db: Session = Depends(get_db),
):
    record = _get_object_or_404(object_id, x_user_id, db)
    if record.status != "ready":
        raise HTTPException(status_code=409, detail="Object is still uploading")
    if record.volume_id is None or record.offset is None:
        # Rows created before Haystack migration only have a local disk path.
        object_path = Path(record.path)
        if not object_path.exists():
            raise HTTPException(status_code=500, detail="Object data missing from disk")

        record.bucket.bandwidth_bytes += record.size
        if x_internal_source:
            record.bucket.internal_transfer_bytes += record.size
        else:
            record.bucket.egress_bytes += record.size
        db.commit()

        return FileResponse(
            path=object_path,
            filename=record.filename,
            media_type="application/octet-stream",
        )

    async with httpx.AsyncClient(base_url=HAYSTACK_BASE_URL, timeout=30.0) as client:
        response = await client.get(
            f"/volume/{record.volume_id}/{record.offset}/{record.size}"
        )
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail="Object data unavailable")

    record.bucket.bandwidth_bytes += record.size
    if x_internal_source:
        record.bucket.internal_transfer_bytes += record.size
    else:
        record.bucket.egress_bytes += record.size
    db.commit()

    filename = record.filename.replace('"', "")
    return Response(
        content=response.content,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.delete(
    "/objects/{object_id}",
    include_in_schema=False,
    response_model=schemas.DeleteResponse,
    tags=["objects"],
    summary="Soft delete an object",
    response_description="Confirmation that the object is now hidden from listings",
)
@app.delete(
    "/files/{object_id}",
    include_in_schema=False,
    response_model=schemas.DeleteResponse,
)
@app.delete(
    "/download/{object_id}",
    response_model=schemas.DeleteResponse,
    tags=["objects"],
    summary="Soft delete an object",
    response_description="Confirmation that the object is now hidden from listings",
)
def delete_object(
    object_id: str,
    x_user_id: str = Header(default="anonymous", min_length=1),
    db: Session = Depends(get_db),
):
    record = _get_object_or_404(object_id, x_user_id, db)
    # Soft delete keeps the bytes on disk and in storage billing so the object
    # can still be recovered later if the product adds an undelete flow.
    record.is_deleted = True
    db.commit()

    return schemas.DeleteResponse(
        object_id=record.id,
        is_deleted=record.is_deleted,
        message=f"Object '{record.filename}' soft deleted.",
    )


@app.get(
    "/admin/volumes/{volume_id}/objects",
    response_model=list[schemas.VolumeObjectRecord],
    tags=["admin"],
    summary="List live objects in a Haystack volume",
)
def list_volume_objects(
    volume_id: int = PathParam(..., ge=1),
    db: Session = Depends(get_db),
):
    records = db.scalars(
        select(models.File)
        .where(
            models.File.volume_id == volume_id,
            models.File.status == "ready",
            models.File.is_deleted.is_(False),
            models.File.offset.is_not(None),
        )
        .order_by(models.File.offset.asc())
    ).all()

    return [
        schemas.VolumeObjectRecord(
            object_id=record.id,
            volume_id=record.volume_id,
            offset=record.offset,
            size=record.size,
        )
        for record in records
        if record.volume_id is not None and record.offset is not None
    ]


@app.patch(
    "/admin/objects/{object_id}/location",
    response_model=schemas.ObjectLocationResponse,
    tags=["admin"],
    summary="Update an object Haystack location",
)
def update_object_location(
    location: schemas.ObjectLocationUpdate,
    object_id: str,
    db: Session = Depends(get_db),
):
    record = db.get(models.File, object_id)
    if record is None or record.is_deleted:
        raise HTTPException(status_code=404, detail="Object not found")

    record.volume_id = location.volume_id
    record.offset = location.offset
    record.size = location.size
    record.status = "ready"
    record.path = haystack_location_path(location.volume_id, location.offset, location.size)
    db.commit()

    return schemas.ObjectLocationResponse(
        object_id=record.id,
        volume_id=record.volume_id,
        offset=record.offset,
        size=record.size,
        status=record.status,
    )


@app.post(
    "/admin/volumes/{volume_id}/compact",
    response_model=schemas.CompactVolumeResponse,
    tags=["admin"],
    summary="Compact a Haystack volume",
)
async def compact_volume(
    volume_id: int = PathParam(..., ge=1),
):
    async with httpx.AsyncClient(base_url=HAYSTACK_BASE_URL, timeout=60.0) as client:
        response = await client.post(f"/admin/volumes/{volume_id}/compact")

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Volume not found")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="Haystack compaction failed")

    return response.json()


@app.get(
    "/buckets/{bucket_id}/billing/",
    response_model=schemas.BucketBillingResponse,
    tags=["buckets"],
    summary="Get bucket billing",
    response_description="Current storage and transfer counters for the selected bucket",
)
def get_bucket_billing(
    bucket_id: int = PathParam(..., ge=1),
    x_user_id: str = Header(default="anonymous", min_length=1),
    db: Session = Depends(get_db),
):
    return _get_bucket_or_404(bucket_id, x_user_id, db)
