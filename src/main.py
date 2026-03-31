import os
import uuid
from typing import List

import aiofiles
from fastapi import Depends, FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

import src.models as models
import src.schemas as schemas
from src.database import Base, engine, get_db

# Create all tables on startup.
Base.metadata.create_all(bind=engine)

STORAGE_DIR = os.path.join(os.path.dirname(__file__), "storage")
os.makedirs(STORAGE_DIR, exist_ok=True)

app = FastAPI(
    title="Object Storage Service",
    description="Simple S3-inspired file storage backend.",
    version="1.0.0",
)


def _get_file_or_404(file_id: str, db: Session) -> models.File:
    record = db.get(models.File, file_id)
    if record is None:
        raise HTTPException(status_code=404, detail="File not found")
    return record


def _assert_owner(record: models.File, user_id: str) -> None:
    if record.user_id != user_id:
        # Return 404 instead of 403 to avoid leaking existence of the file.
        raise HTTPException(status_code=404, detail="File not found")


# ---------------------------------------------------------------------------
# POST /files/upload
# ---------------------------------------------------------------------------


# openapi_extra overrides the schema for the file field from FastAPI's default
# "contentMediaType" (JSON Schema) to "format: binary" (OpenAPI) because
# Swagger UI only recognises the latter and renders a file picker for it.
@app.post(
    "/files/upload",
    response_model=schemas.UploadResponse,
    status_code=201,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "required": ["file"],
                        "properties": {
                            "file": {"type": "string", "format": "binary"}
                        },
                    }
                }
            },
        }
    },
)
async def upload_file(
    file: UploadFile = File(...),
    x_user_id: str = Header(default="anonymous"),
    db: Session = Depends(get_db),
):
    """Upload a file and store it under storage/<user_id>/<file_id>."""
    file_id = str(uuid.uuid4())

    user_dir = os.path.join(STORAGE_DIR, x_user_id)
    os.makedirs(user_dir, exist_ok=True)
    file_path = os.path.join(user_dir, file_id)

    content = await file.read()
    async with aiofiles.open(file_path, "wb") as f:
        await f.write(content)

    record = models.File(
        id=file_id,
        user_id=x_user_id,
        filename=file.filename or file_id,
        path=file_path,
        size=len(content),
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    return schemas.UploadResponse(
        id=record.id,
        filename=record.filename,
        size=record.size,
    )


# ---------------------------------------------------------------------------
# GET /files
# ---------------------------------------------------------------------------


@app.get("/files", response_model=List[schemas.FileRecord])
def list_files(
    x_user_id: str = Header(default="anonymous"),
    db: Session = Depends(get_db),
):
    """Return metadata for all files owned by the requesting user."""
    return db.query(models.File).filter(models.File.user_id == x_user_id).all()


# ---------------------------------------------------------------------------
# GET /files/{id}
# ---------------------------------------------------------------------------


@app.get("/files/{file_id}")
def download_file(
    file_id: str,
    x_user_id: str = Header(default="anonymous"),
    db: Session = Depends(get_db),
):
    """Download the raw file bytes.  Access is restricted to the owning user."""
    record = _get_file_or_404(file_id, db)
    _assert_owner(record, x_user_id)

    if not os.path.exists(record.path):
        raise HTTPException(status_code=500, detail="File data missing from disk")

    return FileResponse(
        path=record.path,
        filename=record.filename,
        media_type="application/octet-stream",
    )


# ---------------------------------------------------------------------------
# DELETE /files/{id}
# ---------------------------------------------------------------------------


@app.delete("/files/{file_id}", response_model=schemas.DeleteResponse)
def delete_file(
    file_id: str,
    x_user_id: str = Header(default="anonymous"),
    db: Session = Depends(get_db),
):
    """Delete a file from disk and remove its metadata record."""
    record = _get_file_or_404(file_id, db)
    _assert_owner(record, x_user_id)

    # Remove bytes from disk first; if this fails we leave the DB record intact
    # so the operator can retry or clean up manually.
    if os.path.exists(record.path):
        os.remove(record.path)

    db.delete(record)
    db.commit()

    return schemas.DeleteResponse(message=f"File '{record.filename}' deleted.")
