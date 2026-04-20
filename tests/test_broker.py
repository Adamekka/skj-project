import time

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete

import src.models as models
from src.broker import manager
from src.broker_protocol import (
    AckMessage,
    DeliverMessage,
    PublishMessage,
    SubscribeMessage,
    SubscribedMessage,
    decode_server_message,
    encode_wire_message,
)
from src.database import SessionLocal
from src.main import app


def _send_client_message(websocket, message, message_format: str) -> None:
    wire_message = encode_wire_message(message, message_format)
    if isinstance(wire_message, bytes):
        websocket.send_bytes(wire_message)
    else:
        websocket.send_text(wire_message)


def _receive_server_message(websocket, message_format: str):
    if message_format == "msgpack":
        return decode_server_message(websocket.receive_bytes())
    return decode_server_message(websocket.receive_text())


def _queued_message_is_delivered(message_id: int) -> bool:
    with SessionLocal() as db:
        queued_message = db.get(models.QueuedMessage, message_id)
        assert queued_message is not None
        return queued_message.is_delivered


def _wait_for_delivery_ack(message_id: int, timeout_seconds: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _queued_message_is_delivered(message_id):
            return True
        time.sleep(0.01)
    return False


@pytest.fixture(autouse=True)
def cleanup_broker_state():
    with SessionLocal() as db:
        db.execute(delete(models.QueuedMessage))
        db.commit()

    manager.reset()
    yield

    with SessionLocal() as db:
        db.execute(delete(models.QueuedMessage))
        db.commit()

    manager.reset()


@pytest.mark.asyncio
async def test_http_app_still_available_via_httpx():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.get("/openapi.json")

    assert response.status_code == 200
    assert response.json()["info"]["title"] == "Object Storage Service"


@pytest.mark.parametrize("message_format", ["json", "msgpack"])
def test_websocket_connect_and_disconnect_cleanup(message_format: str):
    topic = f"connect-{message_format}"

    with TestClient(app) as client:
        with client.websocket_connect(f"/broker?format={message_format}") as websocket:
            _send_client_message(
                websocket,
                SubscribeMessage(action="subscribe", topic=topic),
                message_format,
            )

            subscribed = _receive_server_message(websocket, message_format)
            assert isinstance(subscribed, SubscribedMessage)
            assert subscribed.topic == topic
            assert manager.topic_subscriber_count(topic) == 1

        assert manager.topic_subscriber_count(topic) == 0


@pytest.mark.parametrize("message_format", ["json", "msgpack"])
def test_publish_to_subscribed_topic_delivers_message(message_format: str):
    topic = f"deliver-{message_format}"

    with TestClient(app) as client:
        with client.websocket_connect(f"/broker?format={message_format}") as subscriber:
            with client.websocket_connect(
                f"/broker?format={message_format}"
            ) as publisher:
                _send_client_message(
                    subscriber,
                    SubscribeMessage(action="subscribe", topic=topic),
                    message_format,
                )
                assert isinstance(
                    _receive_server_message(subscriber, message_format),
                    SubscribedMessage,
                )

                _send_client_message(
                    publisher,
                    PublishMessage(
                        action="publish",
                        topic=topic,
                        payload={"temp": 22.5},
                    ),
                    message_format,
                )

                delivered = _receive_server_message(subscriber, message_format)
                assert isinstance(delivered, DeliverMessage)
                assert delivered.topic == topic
                assert delivered.payload == {"temp": 22.5}

                _send_client_message(
                    subscriber,
                    AckMessage(action="ack", message_id=delivered.message_id),
                    message_format,
                )

                assert _wait_for_delivery_ack(delivered.message_id) is True


def test_publish_to_different_topic_is_not_delivered_to_other_subscriber():
    topic_x = "topic-x"
    topic_y = "topic-y"

    with TestClient(app) as client:
        with client.websocket_connect("/broker?format=json") as subscriber:
            with client.websocket_connect("/broker?format=json") as publisher:
                _send_client_message(
                    subscriber,
                    SubscribeMessage(action="subscribe", topic=topic_x),
                    "json",
                )
                assert isinstance(
                    _receive_server_message(subscriber, "json"),
                    SubscribedMessage,
                )

                _send_client_message(
                    publisher,
                    PublishMessage(
                        action="publish",
                        topic=topic_y,
                        payload={"value": "wrong-topic"},
                    ),
                    "json",
                )
                _send_client_message(
                    publisher,
                    PublishMessage(
                        action="publish",
                        topic=topic_x,
                        payload={"value": "expected-topic"},
                    ),
                    "json",
                )

                delivered = _receive_server_message(subscriber, "json")
                assert isinstance(delivered, DeliverMessage)
                assert delivered.topic == topic_x
                assert delivered.payload == {"value": "expected-topic"}


def test_undelivered_message_is_replayed_after_subscribe_and_acknowledged():
    topic = "durable-msgpack"

    with TestClient(app) as client:
        with client.websocket_connect("/broker?format=msgpack") as publisher:
            _send_client_message(
                publisher,
                PublishMessage(
                    action="publish",
                    topic=topic,
                    payload={"status": "queued"},
                ),
                "msgpack",
            )

        with client.websocket_connect("/broker?format=msgpack") as subscriber:
            _send_client_message(
                subscriber,
                SubscribeMessage(action="subscribe", topic=topic),
                "msgpack",
            )

            subscribed = _receive_server_message(subscriber, "msgpack")
            assert isinstance(subscribed, SubscribedMessage)
            assert subscribed.topic == topic

            delivered = _receive_server_message(subscriber, "msgpack")
            assert isinstance(delivered, DeliverMessage)
            assert delivered.payload == {"status": "queued"}

            _send_client_message(
                subscriber,
                AckMessage(action="ack", message_id=delivered.message_id),
                "msgpack",
            )

            assert _wait_for_delivery_ack(delivered.message_id) is True
