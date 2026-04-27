import argparse
import asyncio
from typing import Any

import httpx
import websockets
from pydantic import ValidationError

from src.broker_protocol import (
    AckMessage,
    DeliverMessage,
    ErrorMessage,
    MessageFormat,
    PublishMessage,
    SubscribeMessage,
    SubscribedMessage,
    decode_server_message,
    encode_wire_message,
)
from src.image_processing import (
    IMAGE_DONE_TOPIC,
    IMAGE_JOBS_TOPIC,
    ImageJobCompletedEvent,
    ImageJobFailedEvent,
    ImageProcessJob,
    build_processed_filename,
    process_image_bytes,
)


def _websocket_url(base_url: str, message_format: MessageFormat) -> str:
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}format={message_format}"


async def _send_broker_message(
    websocket: websockets.ClientConnection,
    message: SubscribeMessage | PublishMessage | AckMessage,
    message_format: MessageFormat,
) -> None:
    await websocket.send(encode_wire_message(message, message_format))


async def _publish_done_event(
    websocket: websockets.ClientConnection,
    payload: dict[str, Any],
    message_format: MessageFormat,
) -> None:
    await _send_broker_message(
        websocket,
        PublishMessage(action="publish", topic=IMAGE_DONE_TOPIC, payload=payload),
        message_format,
    )


async def _download_source_object(
    client: httpx.AsyncClient,
    job: ImageProcessJob,
) -> bytes:
    response = await client.get(
        f"/objects/{job.source_object_id}",
        headers={
            "X-User-Id": job.user_id,
            "X-Internal-Source": "true",
        },
    )
    if response.status_code != 200:
        raise ValueError(
            f"Source download failed with status {response.status_code}."
        )
    return response.content


async def _upload_processed_object(
    client: httpx.AsyncClient,
    job: ImageProcessJob,
    image_bytes: bytes,
) -> dict[str, Any]:
    result_filename = build_processed_filename(
        job.source_filename,
        job.request.operation,
    )
    response = await client.post(
        "/objects/upload",
        headers={
            "X-User-Id": job.user_id,
            "X-Internal-Source": "true",
        },
        data={"bucket_id": str(job.source_bucket_id)},
        files={"file": (result_filename, image_bytes, "image/png")},
    )
    if response.status_code != 201:
        raise ValueError(f"Processed upload failed with status {response.status_code}.")
    return response.json()


async def _build_done_payload(
    client: httpx.AsyncClient,
    raw_payload: Any,
) -> dict[str, Any]:
    raw_job = raw_payload if isinstance(raw_payload, dict) else {"raw_payload": raw_payload}

    try:
        job = ImageProcessJob.model_validate(raw_job)
    except ValidationError as exc:
        return ImageJobFailedEvent(
            error=f"Invalid image job payload: {exc}",
            job=raw_job,
        ).model_dump(mode="json")

    try:
        source_bytes = await _download_source_object(client, job)
        processed_bytes = process_image_bytes(source_bytes, job.request)
        upload_payload = await _upload_processed_object(client, job, processed_bytes)
        return ImageJobCompletedEvent(
            source_bucket_id=job.source_bucket_id,
            source_object_id=job.source_object_id,
            source_filename=job.source_filename,
            result_bucket_id=upload_payload["bucket_id"],
            result_object_id=upload_payload["id"],
            result_filename=upload_payload["filename"],
            operation=job.request.operation,
        ).model_dump(mode="json")
    except Exception as exc:
        return ImageJobFailedEvent(error=str(exc), job=job.model_dump(mode="json")).model_dump(
            mode="json"
        )


async def run_worker(
    *,
    http_base_url: str = "http://127.0.0.1:8000",
    broker_url: str = "ws://127.0.0.1:8000/broker",
    message_format: MessageFormat = "json",
    max_jobs: int | None = None,
    ready_event: asyncio.Event | None = None,
    reconnect_delay_seconds: float = 0.2,
) -> int:
    processed_jobs = 0
    websocket_url = _websocket_url(broker_url, message_format)

    async with httpx.AsyncClient(base_url=http_base_url, timeout=30.0) as client:
        while True:
            try:
                async with websockets.connect(websocket_url, max_size=None) as websocket:
                    await _send_broker_message(
                        websocket,
                        SubscribeMessage(action="subscribe", topic=IMAGE_JOBS_TOPIC),
                        message_format,
                    )

                    while True:
                        server_message = decode_server_message(await websocket.recv())

                        if isinstance(server_message, SubscribedMessage):
                            if ready_event is not None and not ready_event.is_set():
                                ready_event.set()
                            continue

                        if isinstance(server_message, ErrorMessage):
                            print(server_message.model_dump_json())
                            continue

                        if not isinstance(server_message, DeliverMessage):
                            continue

                        done_payload = await _build_done_payload(
                            client,
                            server_message.payload,
                        )
                        await _publish_done_event(
                            websocket,
                            done_payload,
                            message_format,
                        )
                        await _send_broker_message(
                            websocket,
                            AckMessage(
                                action="ack",
                                message_id=server_message.message_id,
                            ),
                            message_format,
                        )

                        processed_jobs += 1
                        if max_jobs is not None and processed_jobs >= max_jobs:
                            return processed_jobs
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"Worker connection error: {exc}")
                await asyncio.sleep(reconnect_delay_seconds)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Image processing worker")
    parser.add_argument(
        "--http-base-url",
        default="http://127.0.0.1:8000",
        help="Base HTTP URL of the storage gateway",
    )
    parser.add_argument(
        "--broker-url",
        default="ws://127.0.0.1:8000/broker",
        help="Broker websocket URL",
    )
    parser.add_argument(
        "--format",
        choices=["json", "msgpack"],
        default="json",
        help="Wire format used for broker messages",
    )
    return parser


async def main() -> None:
    args = _build_parser().parse_args()
    await run_worker(
        http_base_url=args.http_base_url,
        broker_url=args.broker_url,
        message_format=args.format,
    )


if __name__ == "__main__":
    asyncio.run(main())
