"""外部事件交换的请求与响应模型。"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ExchangeItemIn(BaseModel):
    client_item_id: str = Field(..., min_length=1, max_length=128)
    event_type: str = Field(..., min_length=1, max_length=64)
    student_id: str = Field(..., min_length=1, max_length=128)
    payload: dict[str, Any] = Field(default_factory=dict)
    retry_of: str | None = Field(default=None, max_length=128)


class ReceiveBatchIn(BaseModel):
    batch_id: str = Field(..., min_length=1, max_length=128)
    plan_version: str = Field(..., min_length=1, max_length=128)
    schema_version: str = Field(..., min_length=1, max_length=16)
    items: list[ExchangeItemIn] = Field(..., max_length=5000)


class SupplementBatchIn(BaseModel):
    items: list[ExchangeItemIn] = Field(..., min_length=1, max_length=5000)


class CloseBatchIn(BaseModel):
    expected_version: int = Field(..., ge=1)


class ReceiptOut(BaseModel):
    item_seq: int
    client_item_id: str
    attempt: int
    status: str
    fingerprint: str
    detail: str
    retry_of: str | None


class BatchHeaderOut(BaseModel):
    batch_id: str
    sender_id: str
    plan_version: str
    schema_version: str
    status: str
    version: int
    intake_fingerprint: str
    content_fingerprint: str
    replay_cursor: int
    item_count: int
    accepted_count: int
    duplicate_count: int
    conflict_count: int
    pending_count: int
    superseded_count: int
    created_at: datetime
    closed_at: datetime | None


class BatchReceiveOut(BatchHeaderOut):
    receipts: list[ReceiptOut]


class ReceiptsPageOut(BatchHeaderOut):
    receipts: list[ReceiptOut]
    next_cursor: int
    has_more: bool


class SupplementOut(BatchHeaderOut):
    receipts: list[ReceiptOut]


class ReconciliationOut(BatchHeaderOut):
    counts_consistent: bool
    event_stream_accepted: int
    balanced: bool
    fingerprint_match: bool | None
    receipts: list[ReceiptOut]
