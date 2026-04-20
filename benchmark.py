import argparse
import asyncio
import time
import uuid

import websockets

from src.broker_protocol import (
    AckMessage,
    DeliverMessage,
    ErrorMessage,
    PublishMessage,
    SubscribeMessage,
    SubscribedMessage,
    decode_server_message,
    encode_wire_message,
)


def _websocket_url(base_url: str, message_format: str) -> str:
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}format={message_format}"


async def _run_single_benchmark(
    base_url: str,
    message_format: str,
    subscriber_count: int,
    publisher_count: int,
    messages_per_publisher: int,
) -> dict[str, float | int | str]:
    websocket_url = _websocket_url(base_url, message_format)
    topic = f"benchmark-{message_format}-{uuid.uuid4().hex}"
    expected_messages_per_subscriber = publisher_count * messages_per_publisher
    ready_event = asyncio.Event()
    ready_lock = asyncio.Lock()
    ready_subscribers = 0

    async def subscriber(index: int) -> int:
        nonlocal ready_subscribers

        async with websockets.connect(
            websocket_url,
            compression=None,
            max_queue=None,
            ping_interval=None,
            ping_timeout=None,
        ) as websocket:
            await websocket.send(
                encode_wire_message(
                    SubscribeMessage(action="subscribe", topic=topic),
                    message_format,
                )
            )

            while True:
                message = decode_server_message(await websocket.recv())
                if isinstance(message, ErrorMessage):
                    raise RuntimeError(
                        f"Subscriber {index} received broker error: {message.detail}"
                    )
                if isinstance(message, SubscribedMessage):
                    async with ready_lock:
                        ready_subscribers += 1
                        if ready_subscribers == subscriber_count:
                            ready_event.set()
                    break

            received_messages = 0
            while received_messages < expected_messages_per_subscriber:
                message = decode_server_message(await websocket.recv())
                if isinstance(message, ErrorMessage):
                    raise RuntimeError(
                        f"Subscriber {index} received broker error: {message.detail}"
                    )
                if not isinstance(message, DeliverMessage):
                    continue

                received_messages += 1
                if index == 0:
                    # The durable queue uses one global delivered flag per message,
                    # so a single ACK is enough to retire it from persistence.
                    await websocket.send(
                        encode_wire_message(
                            AckMessage(action="ack", message_id=message.message_id),
                            message_format,
                        )
                    )

            return received_messages

    async def publisher(index: int) -> int:
        async with websockets.connect(
            websocket_url,
            compression=None,
            max_queue=None,
            ping_interval=None,
            ping_timeout=None,
        ) as websocket:
            for sequence in range(messages_per_publisher):
                await websocket.send(
                    encode_wire_message(
                        PublishMessage(
                            action="publish",
                            topic=topic,
                            payload={"publisher": index, "sequence": sequence},
                        ),
                        message_format,
                    )
                )
        return messages_per_publisher

    subscriber_tasks = [
        asyncio.create_task(subscriber(index)) for index in range(subscriber_count)
    ]
    await asyncio.wait_for(ready_event.wait(), timeout=10)

    start_time = time.perf_counter()
    publisher_results = await asyncio.gather(
        *(publisher(index) for index in range(publisher_count))
    )
    subscriber_results = await asyncio.gather(*subscriber_tasks)
    elapsed_seconds = time.perf_counter() - start_time

    total_published_messages = sum(publisher_results)
    total_delivered_messages = sum(subscriber_results)

    return {
        "format": message_format,
        "subscriber_count": subscriber_count,
        "publisher_count": publisher_count,
        "messages_per_publisher": messages_per_publisher,
        "published_messages": total_published_messages,
        "delivered_messages": total_delivered_messages,
        "elapsed_seconds": elapsed_seconds,
        "throughput_msg_per_s": total_delivered_messages / elapsed_seconds,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark the WebSocket broker")
    parser.add_argument(
        "--url",
        default="ws://127.0.0.1:8000/broker",
        help="Broker websocket URL",
    )
    parser.add_argument(
        "--format",
        choices=["json", "msgpack", "both"],
        default="both",
        help="Wire format to benchmark",
    )
    parser.add_argument(
        "--subscribers",
        type=int,
        default=5,
        help="Number of concurrent subscribers",
    )
    parser.add_argument(
        "--publishers",
        type=int,
        default=5,
        help="Number of concurrent publishers",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=10000,
        help="Messages sent by each publisher",
    )
    return parser


async def main() -> None:
    args = _build_parser().parse_args()
    formats = [args.format] if args.format != "both" else ["json", "msgpack"]

    for message_format in formats:
        result = await _run_single_benchmark(
            base_url=args.url,
            message_format=message_format,
            subscriber_count=args.subscribers,
            publisher_count=args.publishers,
            messages_per_publisher=args.messages,
        )
        print(
            "format={format} subscribers={subscriber_count} publishers={publisher_count} "
            "messages_per_publisher={messages_per_publisher} published={published_messages} "
            "delivered={delivered_messages} elapsed={elapsed_seconds:.3f}s "
            "throughput={throughput_msg_per_s:.2f} msg/s".format(**result)
        )


if __name__ == "__main__":
    asyncio.run(main())
