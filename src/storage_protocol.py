from src.broker_protocol import MessageFormat

STORAGE_WRITE_TOPIC = "storage.write"
STORAGE_ACK_TOPIC = "storage.ack"


def websocket_url(base_url: str, message_format: MessageFormat) -> str:
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}format={message_format}"


def haystack_location_path(volume_id: int, offset: int, size: int) -> str:
    return f"haystack://volume/{volume_id}/{offset}/{size}"
