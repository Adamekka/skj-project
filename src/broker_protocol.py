import base64
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

BINARY_SENTINEL_KEY = "__broker_binary__"
BINARY_VALUE_KEY = "base64"


def encode_binary_values(value: Any) -> Any:
    if isinstance(value, bytes):
        return {
            BINARY_SENTINEL_KEY: True,
            BINARY_VALUE_KEY: base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, list):
        return [encode_binary_values(item) for item in value]
    if isinstance(value, dict):
        return {key: encode_binary_values(item) for key, item in value.items()}
    return value


def decode_binary_values(value: Any) -> Any:
    if isinstance(value, list):
        return [decode_binary_values(item) for item in value]
    if isinstance(value, dict):
        if set(value.keys()) == {BINARY_SENTINEL_KEY, BINARY_VALUE_KEY}:
            if value[BINARY_SENTINEL_KEY] is True and isinstance(
                value[BINARY_VALUE_KEY],
                str,
            ):
                return base64.b64decode(value[BINARY_VALUE_KEY], validate=True)
        return {key: decode_binary_values(item) for key, item in value.items()}
    return value


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
    if message_format == "msgpack":
        payload = message.model_dump(mode="python")
        return msgpack.packb(payload, use_bin_type=True)
    payload = encode_binary_values(message.model_dump(mode="python"))
    return json.dumps(payload)
