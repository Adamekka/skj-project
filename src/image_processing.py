import io
from pathlib import Path
from typing import Annotated, Any, Literal

import numpy as np
from PIL import Image
from pydantic import BaseModel, ConfigDict, Field

IMAGE_JOBS_TOPIC = "image.jobs"
IMAGE_DONE_TOPIC = "image.done"


class ImageProcessingModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class InvertImageRequest(ImageProcessingModel):
    operation: Literal["invert"]


class MirrorImageRequest(ImageProcessingModel):
    operation: Literal["mirror"]


class CropImageRequest(ImageProcessingModel):
    operation: Literal["crop"]
    top: int = Field(..., ge=0)
    left: int = Field(..., ge=0)
    width: int = Field(..., ge=1)
    height: int = Field(..., ge=1)


class BrightnessImageRequest(ImageProcessingModel):
    operation: Literal["brightness"]
    amount: int = Field(..., ge=-255, le=255)


class GrayscaleImageRequest(ImageProcessingModel):
    operation: Literal["grayscale"]


ImageProcessRequest = Annotated[
    InvertImageRequest
    | MirrorImageRequest
    | CropImageRequest
    | BrightnessImageRequest
    | GrayscaleImageRequest,
    Field(discriminator="operation"),
]


class ImageProcessJob(ImageProcessingModel):
    source_bucket_id: int = Field(..., ge=1)
    source_object_id: str = Field(..., min_length=1)
    source_filename: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    request: ImageProcessRequest


class ProcessObjectResponse(ImageProcessingModel):
    status: Literal["processing_started"] = "processing_started"
    bucket_id: int = Field(..., ge=1)
    object_id: str = Field(..., min_length=1)
    topic: str = Field(default=IMAGE_JOBS_TOPIC, min_length=1)
    message_id: int = Field(..., ge=1)


class ImageJobCompletedEvent(ImageProcessingModel):
    status: Literal["completed"] = "completed"
    source_bucket_id: int = Field(..., ge=1)
    source_object_id: str = Field(..., min_length=1)
    source_filename: str = Field(..., min_length=1)
    result_bucket_id: int = Field(..., ge=1)
    result_object_id: str = Field(..., min_length=1)
    result_filename: str = Field(..., min_length=1)
    operation: str = Field(..., min_length=1)


class ImageJobFailedEvent(ImageProcessingModel):
    status: Literal["failed"] = "failed"
    error: str = Field(..., min_length=1)
    job: dict[str, Any]


ImageJobDoneEvent = ImageJobCompletedEvent | ImageJobFailedEvent


def build_processed_filename(source_filename: str, operation: str) -> str:
    source_path = Path(source_filename)
    stem = source_path.stem or source_path.name or "processed"
    return f"{stem}_{operation}.png"


def process_image_bytes(source_bytes: bytes, request: ImageProcessRequest) -> bytes:
    with Image.open(io.BytesIO(source_bytes)) as source_image:
        image_array = np.array(source_image.convert("RGB"), dtype=np.uint8)

    if isinstance(request, InvertImageRequest):
        result_array = 255 - image_array
    elif isinstance(request, MirrorImageRequest):
        result_array = image_array[:, ::-1, :]
    elif isinstance(request, CropImageRequest):
        image_height, image_width = image_array.shape[:2]
        bottom = request.top + request.height
        right = request.left + request.width
        if bottom > image_height or right > image_width:
            raise ValueError("Crop rectangle exceeds image bounds.")
        result_array = image_array[request.top:bottom, request.left:right, :]
    elif isinstance(request, BrightnessImageRequest):
        result_array = np.clip(
            image_array.astype(np.int16) + request.amount,
            0,
            255,
        ).astype(np.uint8)
    elif isinstance(request, GrayscaleImageRequest):
        result_array = np.clip(
            0.299 * image_array[:, :, 0]
            + 0.587 * image_array[:, :, 1]
            + 0.114 * image_array[:, :, 2],
            0,
            255,
        ).astype(np.uint8)
    else:
        raise ValueError(f"Unsupported operation '{request.operation}'.")

    result_image = Image.fromarray(result_array, mode="L" if result_array.ndim == 2 else "RGB")
    output = io.BytesIO()
    result_image.save(output, format="PNG")
    return output.getvalue()
