from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, func, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.database import Base


class Bucket(Base):
    __tablename__ = "buckets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(63), unique=True, nullable=False)
    # Buckets keep the existing X-User-Id ownership model so introducing them
    # does not accidentally make previously private objects globally visible.
    user_id: Mapped[str] = mapped_column(String, index=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )
    bandwidth_bytes: Mapped[int] = mapped_column(
        Integer, server_default=text("0"), nullable=False
    )
    current_storage_bytes: Mapped[int] = mapped_column(
        Integer, server_default=text("0"), nullable=False
    )
    ingress_bytes: Mapped[int] = mapped_column(
        Integer, server_default=text("0"), nullable=False
    )
    egress_bytes: Mapped[int] = mapped_column(
        Integer, server_default=text("0"), nullable=False
    )
    internal_transfer_bytes: Mapped[int] = mapped_column(
        Integer, server_default=text("0"), nullable=False
    )
    files: Mapped[list["File"]] = relationship(back_populates="bucket")


class File(Base):
    __tablename__ = "files"

    id: Mapped[str] = mapped_column(String, primary_key=True, index=True)
    user_id: Mapped[str] = mapped_column(String, index=True, nullable=False)
    bucket_id: Mapped[int] = mapped_column(
        ForeignKey("buckets.id"), index=True, nullable=False
    )
    filename: Mapped[str] = mapped_column(String, nullable=False)
    # Absolute path on disk where the raw bytes are stored.
    path: Mapped[str] = mapped_column(String, nullable=False)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )
    is_deleted: Mapped[bool] = mapped_column(
        Boolean, server_default=text("0"), nullable=False
    )
    bucket: Mapped[Bucket] = relationship(back_populates="files")


class QueuedMessage(Base):
    __tablename__ = "queued_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String, index=True, nullable=False)
    payload: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )
    is_delivered: Mapped[bool] = mapped_column(
        Boolean, server_default=text("0"), nullable=False
    )
