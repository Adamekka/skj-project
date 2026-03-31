from datetime import datetime

from pydantic import BaseModel, Field


class UploadResponse(BaseModel):
    """Returned immediately after a successful upload."""

    id: str = Field(
        ...,
        title="File ID",
        description="Unique UUID identifier assigned to the uploaded file.",
    )
    filename: str = Field(
        ...,
        title="Filename",
        description="Original name of the file as supplied by the client.",
        min_length=1,
    )
    size: int = Field(
        ...,
        title="File Size",
        description="Size of the stored file in bytes.",
        ge=0,
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "filename": "report.pdf",
                "size": 204800,
            }
        }
    }


class FileRecord(BaseModel):
    """Full metadata record returned by the listing and info endpoints."""

    id: str = Field(
        ...,
        title="File ID",
        description="Unique UUID identifier of the file.",
    )
    user_id: str = Field(
        ...,
        title="Owner",
        description="Identifier of the user who uploaded the file (from X-User-Id header).",
        min_length=1,
    )
    filename: str = Field(
        ...,
        title="Filename",
        description="Original name of the file as supplied by the client.",
        min_length=1,
    )
    size: int = Field(
        ...,
        title="File Size",
        description="Size of the file in bytes.",
        ge=0,
    )
    created_at: datetime = Field(
        ...,
        title="Upload Time",
        description="UTC timestamp when the file was uploaded.",
    )

    model_config = {
        "from_attributes": True,
        "json_schema_extra": {
            "example": {
                "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "user_id": "alice",
                "filename": "report.pdf",
                "size": 204800,
                "created_at": "2026-03-31T12:00:00Z",
            }
        },
    }


class DeleteResponse(BaseModel):
    """Returned after a file is successfully deleted."""

    message: str = Field(
        ...,
        title="Confirmation",
        description="Human-readable confirmation of the deletion.",
        min_length=1,
    )

    model_config = {
        "json_schema_extra": {
            "example": {"message": "File 'report.pdf' deleted."}
        }
    }
