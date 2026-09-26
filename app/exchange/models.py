"""外部事件交换批次与逐条回执的持久化模型。"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from ..models import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ExchangeBatch(Base):
    """一次版本化的外部事件交换批次。"""

    __tablename__ = "exchange_batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    batch_id: Mapped[str] = mapped_column(String(128), nullable=False)
    sender_id: Mapped[str] = mapped_column(String(128), nullable=False)
    plan_version: Mapped[str] = mapped_column(String(128), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open")
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    intake_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    content_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    replay_cursor: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    item_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    accepted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    conflict_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pending_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    superseded_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    closed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("sender_id", "batch_id", name="uq_exchange_batches_sender_batch"),
        CheckConstraint("version >= 1", name="ck_exchange_batches_version_pos"),
        CheckConstraint("replay_cursor >= 0", name="ck_exchange_batches_cursor_nonneg"),
    )


class ExchangeItem(Base):
    """批次内逐条回执，superseded 条目保留为审计轨迹。"""

    __tablename__ = "exchange_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    batch_fk: Mapped[int] = mapped_column(
        ForeignKey("exchange_batches.id"), nullable=False
    )
    item_seq: Mapped[int] = mapped_column(Integer, nullable=False)
    client_item_id: Mapped[str] = mapped_column(String(128), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    student_id: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    detail: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    retry_of: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("batch_fk", "item_seq", name="uq_exchange_items_batch_seq"),
        UniqueConstraint(
            "batch_fk", "client_item_id", "attempt", name="uq_exchange_items_batch_attempt"
        ),
        Index("ix_exchange_items_batch_status", "batch_fk", "status"),
    )
