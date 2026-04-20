import uuid
from pathlib import Path
from typing import Any

import aiofiles
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
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from src.broker import router as broker_router
import src.models as models
import src.schemas as schemas
from src.database import get_db

STORAGE_DIR = Path(__file__).resolve().parent.parent / "storage"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(
    title="Object Storage Service",
    description="Simple S3-inspired file storage backend with buckets and billing.",
    version="2.0.0",
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
    status_code=201,
    tags=["objects"],
    summary="Upload an object",
    response_description="Metadata of the newly stored object",
)
@app.post(
    "/files/upload",
    include_in_schema=False,
    response_model=schemas.ObjectUploadResponse,
    status_code=201,
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

    bucket_dir = STORAGE_DIR / x_user_id / str(bucket.id)
    bucket_dir.mkdir(parents=True, exist_ok=True)
    object_path = bucket_dir / object_id

    content = await file.read()
    async with aiofiles.open(object_path, "wb") as output_file:
        await output_file.write(content)

    record = models.File(
        id=object_id,
        user_id=x_user_id,
        bucket_id=bucket.id,
        filename=file.filename or object_id,
        path=str(object_path),
        size=len(content),
    )

    bucket.current_storage_bytes += record.size
    bucket.bandwidth_bytes += record.size
    if x_internal_source:
        bucket.internal_transfer_bytes += record.size
    else:
        bucket.ingress_bytes += record.size

    db.add(record)

    try:
        db.commit()
    except Exception:
        db.rollback()
        if object_path.exists():
            object_path.unlink()
        raise

    db.refresh(record)
    return schemas.ObjectUploadResponse(
        id=record.id,
        bucket_id=record.bucket_id,
        filename=record.filename,
        size=record.size,
    )


@app.get(
    "/objects/{object_id}",
    tags=["objects"],
    summary="Download an object",
    response_description="Raw object bytes as an octet-stream",
)
@app.get("/files/{object_id}", include_in_schema=False)
def download_object(
    object_id: str,
    x_user_id: str = Header(default="anonymous", min_length=1),
    x_internal_source: bool = Header(default=False),
    db: Session = Depends(get_db),
):
    record = _get_object_or_404(object_id, x_user_id, db)
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


@app.delete(
    "/objects/{object_id}",
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
