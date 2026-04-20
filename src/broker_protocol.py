import json
from typing import Any, Annotated, Literal

import msgpack
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

MessageFormat = Literal["json", "msgpack"]


class BrokerMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SubscribeMessage(BrokerMessage):
    action: Literal["subscribe"]
    topic: str = Field(..., min_length=1, max_length=255)


class PublishMessage(BrokerMessage):
    action: Literal["publish"]
    topic: str = Field(..., min_length=1, max_length=255)
    payload: Any


class AckMessage(BrokerMessage):
    action: Literal["ack"]
    message_id: int = Field(..., ge=1)


class SubscribedMessage(BrokerMessage):
    action: Literal["subscribed"] = "subscribed"
    topic: str = Field(..., min_length=1, max_length=255)


class DeliverMessage(BrokerMessage):
    action: Literal["deliver"] = "deliver"
    topic: str = Field(..., min_length=1, max_length=255)
    message_id: int = Field(..., ge=1)
    payload: Any


class ErrorMessage(BrokerMessage):
    action: Literal["error"] = "error"
    detail: str = Field(..., min_length=1)


InboundBrokerMessage = Annotated[
    SubscribeMessage | PublishMessage | AckMessage,
    Field(discriminator="action"),
]
OutboundBrokerMessage = Annotated[
    SubscribedMessage | DeliverMessage | ErrorMessage,
    Field(discriminator="action"),
]

INBOUND_BROKER_MESSAGE_ADAPTER = TypeAdapter(InboundBrokerMessage)
OUTBOUND_BROKER_MESSAGE_ADAPTER = TypeAdapter(OutboundBrokerMessage)


def normalize_message_format(raw_format: str | None) -> MessageFormat:
    if raw_format == "msgpack":
        return "msgpack"
    if raw_format in {None, "json"}:
        return "json"
    raise ValueError("Unsupported format. Use 'json' or 'msgpack'.")


def decode_wire_message(
    message: str | bytes,
) -> SubscribeMessage | PublishMessage | AckMessage:
    if isinstance(message, str):
        decoded = json.loads(message)
    else:
        decoded = msgpack.unpackb(message, raw=False)
    return INBOUND_BROKER_MESSAGE_ADAPTER.validate_python(decoded)


def decode_server_message(
    message: str | bytes,
) -> SubscribedMessage | DeliverMessage | ErrorMessage:
    if isinstance(message, str):
        decoded = json.loads(message)
    else:
        decoded = msgpack.unpackb(message, raw=False)
    return OUTBOUND_BROKER_MESSAGE_ADAPTER.validate_python(decoded)


def encode_wire_message(
    message: BrokerMessage,
    message_format: MessageFormat,
) -> str | bytes:
    payload = message.model_dump(mode="json")
    if message_format == "msgpack":
        return msgpack.packb(payload, use_bin_type=True)
    return json.dumps(payload)
