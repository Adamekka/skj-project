from datetime import datetime

from pydantic import BaseModel, Field


class BucketCreate(BaseModel):
    name: str = Field(
        ...,
        title="Bucket Name",
        description="Globally unique bucket name.",
        min_length=3,
        max_length=63,
        pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$",
    )

    model_config = {"json_schema_extra": {"example": {"name": "project-assets"}}}


class BucketRecord(BaseModel):
    id: int = Field(
        ...,
        title="Bucket ID",
        description="Numeric identifier of the bucket.",
        ge=1,
    )
    name: str = Field(
        ...,
        title="Bucket Name",
        description="Globally unique bucket name.",
        min_length=3,
        max_length=63,
    )
    user_id: str = Field(
        ...,
        title="Bucket Owner",
        description="User identifier taken from the X-User-Id header.",
        min_length=1,
    )
    created_at: datetime = Field(
        ...,
        title="Created At",
        description="UTC timestamp when the bucket was created.",
    )

    model_config = {
        "from_attributes": True,
        "json_schema_extra": {
            "example": {
                "id": 1,
                "name": "project-assets",
                "user_id": "alice",
                "created_at": "2026-04-09T20:00:00Z",
            }
        },
    }


class ObjectUploadRequest(BaseModel):
    bucket_id: int = Field(
        ...,
        title="Bucket ID",
        description="Identifier of the target bucket for the uploaded object.",
        ge=1,
    )

    model_config = {"json_schema_extra": {"example": {"bucket_id": 1}}}


class ObjectUploadResponse(BaseModel):
    id: str = Field(
        ...,
        title="Object ID",
        description="Unique UUID identifier assigned to the uploaded object.",
    )
    bucket_id: int = Field(
        ...,
        title="Bucket ID",
        description="Identifier of the bucket where the object is stored.",
        ge=1,
    )
    filename: str = Field(
        ...,
        title="Filename",
        description="Original name of the uploaded file.",
        min_length=1,
    )
    size: int = Field(
        ...,
        title="Object Size",
        description="Size of the uploaded object in bytes.",
        ge=0,
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "bucket_id": 1,
                "filename": "invoice.pdf",
                "size": 204800,
            }
        }
    }


class ObjectRecord(BaseModel):
    id: str = Field(
        ...,
        title="Object ID",
        description="Unique UUID identifier of the stored object.",
    )
    bucket_id: int = Field(
        ...,
        title="Bucket ID",
        description="Identifier of the bucket that owns the object.",
        ge=1,
    )
    user_id: str = Field(
        ...,
        title="Object Owner",
        description="User identifier taken from the X-User-Id header.",
        min_length=1,
    )
    filename: str = Field(
        ...,
        title="Filename",
        description="Original name of the uploaded file.",
        min_length=1,
    )
    size: int = Field(
        ...,
        title="Object Size",
        description="Size of the stored object in bytes.",
        ge=0,
    )
    created_at: datetime = Field(
        ...,
        title="Created At",
        description="UTC timestamp when the object was stored.",
    )

    model_config = {
        "from_attributes": True,
        "json_schema_extra": {
            "example": {
                "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "bucket_id": 1,
                "user_id": "alice",
                "filename": "invoice.pdf",
                "size": 204800,
                "created_at": "2026-04-09T20:05:00Z",
            }
        },
    }


class BucketBillingResponse(BaseModel):
    id: int = Field(
        ...,
        title="Bucket ID",
        description="Numeric identifier of the bucket.",
        ge=1,
    )
    name: str = Field(
        ...,
        title="Bucket Name",
        description="Bucket name used for billing reporting.",
        min_length=3,
        max_length=63,
    )
    bandwidth_bytes: int = Field(
        ...,
        title="Total Bandwidth",
        description="Total tracked transfer volume in bytes across uploads and downloads.",
        ge=0,
    )
    current_storage_bytes: int = Field(
        ...,
        title="Current Storage",
        description="Current bytes stored in the bucket.",
        ge=0,
    )
    ingress_bytes: int = Field(
        ...,
        title="Ingress",
        description="Cumulative bytes received from external clients.",
        ge=0,
    )
    egress_bytes: int = Field(
        ...,
        title="Egress",
        description="Cumulative bytes sent to external clients.",
        ge=0,
    )
    internal_transfer_bytes: int = Field(
        ...,
        title="Internal Transfer",
        description="Cumulative bytes transferred internally inside the cloud.",
        ge=0,
    )

    model_config = {
        "from_attributes": True,
        "json_schema_extra": {
            "example": {
                "id": 1,
                "name": "project-assets",
                "bandwidth_bytes": 409600,
                "current_storage_bytes": 204800,
                "ingress_bytes": 204800,
                "egress_bytes": 204800,
                "internal_transfer_bytes": 0,
            }
        },
    }


class DeleteResponse(BaseModel):
    object_id: str = Field(
        ...,
        title="Object ID",
        description="Identifier of the object that was soft deleted.",
    )
    is_deleted: bool = Field(
        ...,
        title="Soft Deleted",
        description="Whether the object is now marked as deleted.",
    )
    message: str = Field(
        ...,
        title="Confirmation",
        description="Human-readable confirmation of the soft delete operation.",
        min_length=1,
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "object_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "is_deleted": True,
                "message": "Object 'invoice.pdf' soft deleted.",
            }
        }
    }
