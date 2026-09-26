"""服务端业务模块。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class PlanIn(BaseModel):
    plan_version: str = Field(..., min_length=1, max_length=128)
    iana_timezone: str = Field(..., min_length=1, max_length=64)
    required_seconds: int = Field(0, ge=0)


class PlanOut(BaseModel):
    plan_version: str
    iana_timezone: str
    required_seconds: int


class CheckinPayload(BaseModel):
    activity_id: str = ""
    activity_type: str = "regular"
    check_in_at: datetime
    check_out_at: datetime

    @model_validator(mode="after")
    def _check_order(self) -> "CheckinPayload":
        if self.check_out_at <= self.check_in_at:
            raise ValueError("check_out_at must be after check_in_at")
        return self

    @field_validator("check_in_at", "check_out_at")
    @classmethod
    def _ensure_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware (RFC 3339)")
        return v


class MentorConfirmPayload(BaseModel):
    checkin_event_id: str


class LeaveCorrectionPayload(BaseModel):
    adjustment_seconds: int
    reason: str = ""


class EventIn(BaseModel):
    event_id: str = Field(..., min_length=1, max_length=128)
    event_type: Literal["checkin", "mentor_confirm", "leave_correction"]
    student_id: str = Field(..., min_length=1, max_length=128)
    payload: dict[str, Any]


class EventBatchIn(BaseModel):
    events: list[EventIn]


class EventOut(BaseModel):
    event_id: str
    plan_version: str
    event_type: str
    student_id: str
    payload: dict[str, Any]
    created_at: datetime

    model_config = {"from_attributes": True}


class ImportResult(BaseModel):
    accepted: int
    duplicates: list[str]
    rejected: list[dict[str, Any]]


class DailyTotal(BaseModel):
    academic_day: str
    seconds: int


class CheckinExplanation(BaseModel):
    event_id: str
    activity_id: str
    activity_type: str
    status: str
    counts: bool
    check_in_at_utc: str
    check_out_at_utc: str
    raw_seconds: int
    academic_days: list[dict[str, Any]]


class AdjustmentOut(BaseModel):
    event_id: str
    seconds: int
    reason: str


class StudentProgressOut(BaseModel):
    student_id: str
    confirmed_seconds: int
    pending_seconds: int
    adjustment_seconds: int
    total_seconds: int
    lesson_units: int
    pending_lesson_units: int
    meets_requirement: bool
    daily: list[DailyTotal]
    checkins: list[CheckinExplanation]
    adjustments: list[AdjustmentOut]


class SnapshotOut(BaseModel):
    plan_version: str
    freeze_id: str | None
    timezone: str
    required_seconds: int
    generated_at: str
    event_cutoff_id: str | None
    students: list[dict[str, Any]]


class FreezeIn(BaseModel):
    pass


class DiffOut(BaseModel):
    plan_version: str
    old_freeze_id: str | None
    new_freeze_id: str | None
    old_generated_at: str
    new_generated_at: str
    old_event_cutoff_id: str | None
    new_event_cutoff_id: str | None
    student_changes: list[dict[str, Any]]
    students_affected: int


# ---------------------------------------------------------------------------
# 合作院校事件交换（版本化批次 + 逐条回执）
# ---------------------------------------------------------------------------

EXCHANGE_SCHEMA_VERSION = "v1"


class ExchangeItemIn(BaseModel):
    """批次中的单条事件。``client_line_id`` 为发送方自带的可选行标识。"""

    event_id: str = Field(..., min_length=1, max_length=128)
    event_type: Literal["checkin", "mentor_confirm", "leave_correction"]
    student_id: str = Field(..., min_length=1, max_length=128)
    payload: dict[str, Any]
    client_line_id: str = Field("", max_length=128)


class ExchangeBatchIn(BaseModel):
    batch_id: str = Field(..., min_length=1, max_length=128)
    events: list[ExchangeItemIn] = Field(default_factory=list)


class ExchangeSupplementItemIn(ExchangeItemIn):
    """补交条目：引用原批次行号进行修正/确认，或不引用以补交新条目。"""

    references: int | None = Field(None, ge=1)
    resolution: Literal["confirm"] | None = None


class ExchangeSupplementIn(BaseModel):
    events: list[ExchangeSupplementItemIn] = Field(default_factory=list)


class ExchangeReceiptItemOut(BaseModel):
    line_no: int
    revision: int
    event_id: str
    event_type: str
    student_id: str
    status: str
    reason_code: str | None = None
    reason_detail: str | None = None
    references_line: int | None = None
    resolved_by_line: int | None = None
    client_line_id: str = ""
    payload_fingerprint: str


class ExchangeBatchOut(BaseModel):
    sender_id: str
    batch_id: str
    plan_version: str
    schema_version: str
    state: str
    revision: int
    content_fingerprint: str
    cursor: int
    total_count: int
    accepted_count: int
    duplicate_count: int
    conflict_count: int
    pending_count: int
    resolved_count: int = 0
    closed: bool = False


class ExchangeReceiptOut(BaseModel):
    batch: ExchangeBatchOut
    items: list[ExchangeReceiptItemOut]
    next_cursor: int | None = None


class ExchangeAcceptedOut(BaseModel):
    line_no: int
    event_id: str


class ExchangeReceiveOut(BaseModel):
    batch: ExchangeBatchOut
    replay: bool
    resumed: bool
    accepted: list[ExchangeAcceptedOut]
    duplicates: list[int]
    conflicts: list[int]
    pending_review: list[int]


class ExchangeCloseIn(BaseModel):
    force: bool = False
    reason: str = Field("", max_length=256)


class ExchangeCloseOut(BaseModel):
    batch: ExchangeBatchOut
    closed: bool
    unresolved_conflicts: int
    unresolved_pending: int


class ExchangeReconcileItemOut(BaseModel):
    line_no: int
    status: str
    event_id: str
    issue: str | None = None
    event_row_id: int | None = None


class ExchangeReconcileOut(BaseModel):
    batch: ExchangeBatchOut
    fingerprint_ok: bool
    cursor_complete: bool
    events_expected: int
    events_found: int
    balanced: bool
    items: list[ExchangeReconcileItemOut]
