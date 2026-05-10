import argparse
import asyncio
import os
from pathlib import Path
from typing import Any

import httpx

DEFAULT_VOLUME_DIR = Path(__file__).resolve().parent / "haystack_volumes"


async def compact_volume(
    volume_id: int,
    volume_dir: Path,
    gateway_url: str,
) -> int:
    source_path = volume_dir / f"volume_{volume_id}.dat"
    compacted_path = volume_dir / f"volume_{volume_id}_compacted.dat"
    if not source_path.exists():
        raise FileNotFoundError(source_path)

    async with httpx.AsyncClient(base_url=gateway_url, timeout=30.0) as client:
        response = await client.get(f"/admin/volumes/{volume_id}/objects")
        response.raise_for_status()
        objects: list[dict[str, Any]] = response.json()

        with open(source_path, "rb") as source_file:
            with open(compacted_path, "wb") as compacted_file:
                for record in objects:
                    source_file.seek(record["offset"])
                    data = source_file.read(record["size"])
                    if len(data) != record["size"]:
                        raise ValueError(
                            f"Object {record['object_id']} is missing bytes in volume {volume_id}."
                        )

                    new_offset = compacted_file.tell()
                    compacted_file.write(data)
                    update = await client.patch(
                        f"/admin/objects/{record['object_id']}/location",
                        json={
                            "volume_id": volume_id,
                            "offset": new_offset,
                            "size": record["size"],
                        },
                    )
                    update.raise_for_status()

    compacted_path.replace(source_path)
    return len(objects)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compact one Haystack volume")
    parser.add_argument("volume_id", type=int, help="Numeric Haystack volume ID")
    parser.add_argument(
        "--volume-dir",
        default=os.getenv("HAYSTACK_VOLUME_DIR", str(DEFAULT_VOLUME_DIR)),
        help="Directory containing volume_N.dat files",
    )
    parser.add_argument(
        "--gateway-url",
        default=os.getenv("S3_GATEWAY_URL", "http://127.0.0.1:8000"),
        help="Base URL of the S3 gateway API",
    )
    return parser


async def main() -> None:
    args = _build_parser().parse_args()
    moved_objects = await compact_volume(
        args.volume_id,
        Path(args.volume_dir),
        args.gateway_url,
    )
    print(f"Compacted volume {args.volume_id}: moved {moved_objects} live objects.")


if __name__ == "__main__":
    asyncio.run(main())
