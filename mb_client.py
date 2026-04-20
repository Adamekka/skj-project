import argparse
import asyncio
import json
from typing import Any

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


async def _publish_messages(args: argparse.Namespace) -> None:
    payload: Any = json.loads(args.payload)
    websocket_url = _websocket_url(args.url, args.format)

    async with websockets.connect(websocket_url) as websocket:
        for _ in range(args.count):
            await websocket.send(
                encode_wire_message(
                    PublishMessage(
                        action="publish",
                        topic=args.topic,
                        payload=payload,
                    ),
                    args.format,
                )
            )


async def _subscribe_messages(args: argparse.Namespace) -> None:
    websocket_url = _websocket_url(args.url, args.format)
    received_messages = 0

    async with websockets.connect(websocket_url) as websocket:
        await websocket.send(
            encode_wire_message(
                SubscribeMessage(action="subscribe", topic=args.topic),
                args.format,
            )
        )

        while args.limit <= 0 or received_messages < args.limit:
            message = decode_server_message(await websocket.recv())

            if isinstance(message, SubscribedMessage):
                print(message.model_dump_json())
                continue

            if isinstance(message, ErrorMessage):
                print(message.model_dump_json())
                continue

            if isinstance(message, DeliverMessage):
                print(message.model_dump_json())
                received_messages += 1

                if args.ack:
                    await websocket.send(
                        encode_wire_message(
                            AckMessage(action="ack", message_id=message.message_id),
                            args.format,
                        )
                    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Simple broker client")
    parser.add_argument(
        "--url",
        default="ws://127.0.0.1:8000/broker",
        help="Broker websocket URL",
    )
    parser.add_argument(
        "--format",
        choices=["json", "msgpack"],
        default="json",
        help="Wire format used for websocket messages",
    )

    subparsers = parser.add_subparsers(dest="mode", required=True)

    publish_parser = subparsers.add_parser("publish", help="Run as publisher")
    publish_parser.add_argument("--topic", required=True, help="Target topic")
    publish_parser.add_argument(
        "--payload",
        required=True,
        help="JSON payload string, for example '{\"temp\": 22.5}'",
    )
    publish_parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of publish messages to send",
    )

    subscribe_parser = subparsers.add_parser("subscribe", help="Run as subscriber")
    subscribe_parser.add_argument("--topic", required=True, help="Subscribed topic")
    subscribe_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Stop after receiving this many deliver messages, 0 means forever",
    )
    subscribe_parser.add_argument(
        "--ack",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Send ACKs for delivered messages",
    )

    return parser


async def main() -> None:
    args = _build_parser().parse_args()
    if args.mode == "publish":
        await _publish_messages(args)
        return

    await _subscribe_messages(args)


if __name__ == "__main__":
    asyncio.run(main())
