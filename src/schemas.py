from datetime import datetime

from pydantic import BaseModel


class UploadResponse(BaseModel):
    """Returned immediately after a successful upload."""

    id: str
    filename: str
    size: int


class FileRecord(BaseModel):
    """Full metadata record returned by GET /files and GET /files/{id} info."""

    id: str
    user_id: str
    filename: str
    size: int
    created_at: datetime

    model_config = {"from_attributes": True}


class DeleteResponse(BaseModel):
    message: str
