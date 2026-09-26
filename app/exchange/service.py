"""版本化外部事件交换：批次接收、逐条回执、补交、关闭与对账。

设计要点：
- 批次按 (sender_id, batch_id) 唯一，发送方只能访问自己的批次；
- intake_fingerprint 在创建时固定，用于幂等重放判定；content_fingerprint
  是逐条链式 sha256，随补交继续延伸，用于对账；
- replay_cursor 是批内单调条目序号，回执接口按游标增量拉取；
- 已接受条目写入事件流 events 表，唯一约束兜底，绝不重复写入；
- 冲突与待审核条目可通过补交引用原 client_item_id 修正重试，被取代的
  条目转为 superseded 保留审计轨迹；
- 关闭采用版本号乐观锁，并发关闭只有一个成功。
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from ..core.replay import EventType
from ..models import Event as EventModel
from ..repository import get_plan
from .models import ExchangeBatch, ExchangeItem

BATCH_OPEN = "open"
BATCH_SEALED = "sealed"

ITEM_ACCEPTED = "accepted"
ITEM_DUPLICATE = "duplicate"
ITEM_CONFLICT = "conflict"
ITEM_PENDING = "pending_review"
ITEM_SUPERSEDED = "superseded"

CORRECTABLE_STATUSES = frozenset({ITEM_CONFLICT, ITEM_PENDING})

SUPPORTED_SCHEMA_VERSIONS = frozenset({"1.0"})

KNOWN_EVENT_TYPES = frozenset(e.value for e in EventType)

GENESIS_FINGERPRINT = "0" * 64

MAX_RECEIPT_PAGE = 1000


class ExchangeError(Exception):
    """交换层业务错误基类。"""


class BatchNotFoundError(ExchangeError):
    pass


class BatchClosedError(ExchangeError):
    pass


class VersionConflictError(ExchangeError):
    pass


class BatchFingerprintMismatchError(ExchangeError):
    pass


class UnsupportedSchemaError(ExchangeError):
    pass


class PlanNotFoundError(ExchangeError):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_item_fingerprint(
    *,
    plan_version: str,
    client_item_id: str,
    event_type: str,
    student_id: str,
    payload: dict[str, Any],
) -> str:
    """单条内容的确定性指纹，与事件流比对时口径一致。"""
    raw = _canonical_json(
        {
            "plan_version": plan_version,
            "client_item_id": client_item_id,
            "event_type": event_type,
            "student_id": student_id,
            "payload": payload,
        }
    )
    return sha256(raw.encode("utf-8")).hexdigest()


def chain_fingerprint(previous: str, item_fingerprint: str) -> str:
    """把单条指纹链接进整批指纹，顺序敏感。"""
    return sha256(f"{previous}|{item_fingerprint}".encode("utf-8")).hexdigest()


def _fingerprint_items(
    plan_version: str, items: list[dict[str, Any]]
) -> tuple[str, list[str]]:
    fingerprints = [
        compute_item_fingerprint(
            plan_version=plan_version,
            client_item_id=item["client_item_id"],
            event_type=item["event_type"],
            student_id=item["student_id"],
            payload=item["payload"],
        )
        for item in items
    ]
    chain = GENESIS_FINGERPRINT
    for fingerprint in fingerprints:
        chain = chain_fingerprint(chain, fingerprint)
    return chain, fingerprints


def _validate_payload(event_type: str, payload: dict[str, Any]) -> str | None:
    """结构校验，返回 None 表示合法，否则返回冲突原因。"""
    if event_type == EventType.CHECKIN.value:
        try:
            start = datetime.fromisoformat(str(payload["check_in_at"]))
            end = datetime.fromisoformat(str(payload["check_out_at"]))
        except (KeyError, TypeError, ValueError):
            return "checkin payload requires RFC 3339 check_in_at/check_out_at"
        if start.tzinfo is None or end.tzinfo is None:
            return "checkin timestamps must be timezone-aware"
        if end <= start:
            return "check_out_at must be after check_in_at"
        return None
    if event_type == EventType.MENTOR_CONFIRM.value:
        target = payload.get("checkin_event_id")
        if not isinstance(target, str) or not target.strip():
            return "mentor_confirm payload requires a non-empty checkin_event_id"
        return None
    if event_type == EventType.LEAVE_CORRECTION.value:
        seconds = payload.get("adjustment_seconds")
        if isinstance(seconds, bool) or not isinstance(seconds, int):
            return "leave_correction payload requires an integer adjustment_seconds"
        return None
    return None


def _get_event(db: Session, plan_version: str, event_id: str) -> EventModel | None:
    stmt = select(EventModel).where(
        EventModel.plan_version == plan_version,
        EventModel.event_id == event_id,
    )
    return db.execute(stmt).scalar_one_or_none()


def _event_fingerprint(plan_version: str, event: EventModel) -> str:
    return compute_item_fingerprint(
        plan_version=plan_version,
        client_item_id=event.event_id,
        event_type=event.event_type,
        student_id=event.student_id,
        payload=event.payload,
    )


def _insert_event(db: Session, *, plan_version: str, item: dict[str, Any]) -> bool:
    """写入事件流，唯一约束兜底，已接受项绝不重复写入。"""
    stmt = sqlite_insert(EventModel).values(
        event_id=item["client_item_id"],
        plan_version=plan_version,
        student_id=item["student_id"],
        event_type=item["event_type"],
        payload=item["payload"],
    )
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["event_id", "plan_version"]
    ).returning(EventModel.id)
    return db.execute(stmt).scalar_one_or_none() is not None


def _classify(
    db: Session,
    *,
    plan_version: str,
    item: dict[str, Any],
    fingerprint: str,
    in_batch: dict[str, str],
    accepted_ids: set[str],
) -> tuple[str, str]:
    """把单条条目分类为四种回执状态之一。"""
    event_type = item["event_type"]
    if event_type not in KNOWN_EVENT_TYPES:
        return ITEM_PENDING, f"unknown event_type '{event_type}' requires manual review"
    invalid = _validate_payload(event_type, item["payload"])
    if invalid is not None:
        return ITEM_CONFLICT, invalid
    seen = in_batch.get(item["client_item_id"])
    if seen is not None:
        if seen == fingerprint:
            return ITEM_DUPLICATE, "identical item already received in this batch"
        return (
            ITEM_CONFLICT,
            "conflicting payload for the same client_item_id within this batch",
        )
    existing = _get_event(db, plan_version, item["client_item_id"])
    if existing is not None:
        if _event_fingerprint(plan_version, existing) == fingerprint:
            return ITEM_DUPLICATE, "identical event already accepted into the event stream"
        return ITEM_CONFLICT, "payload conflicts with an already accepted event"
    if event_type == EventType.MENTOR_CONFIRM.value:
        target = item["payload"]["checkin_event_id"]
        if target not in accepted_ids and _get_event(db, plan_version, target) is None:
            return ITEM_PENDING, f"referenced checkin '{target}' is not accepted yet"
    return ITEM_ACCEPTED, "accepted into the event stream"


def _persist_accepted(
    db: Session, *, plan_version: str, item: dict[str, Any], fingerprint: str
) -> tuple[str, str]:
    """落库已接受条目；并发下唯一约束未命中时按现状重新判定。"""
    if _insert_event(db, plan_version=plan_version, item=item):
        return ITEM_ACCEPTED, "accepted into the event stream"
    existing = _get_event(db, plan_version, item["client_item_id"])
    if existing is not None and _event_fingerprint(plan_version, existing) == fingerprint:
        return ITEM_DUPLICATE, "identical event already accepted into the event stream"
    return ITEM_CONFLICT, "payload conflicts with an already accepted event"


def _find_batch(db: Session, *, sender_id: str, batch_id: str) -> ExchangeBatch | None:
    stmt = select(ExchangeBatch).where(
        ExchangeBatch.sender_id == sender_id,
        ExchangeBatch.batch_id == batch_id,
    )
    return db.execute(stmt).scalar_one_or_none()


def _batch_items(db: Session, batch_fk: int) -> list[ExchangeItem]:
    stmt = (
        select(ExchangeItem)
        .where(ExchangeItem.batch_fk == batch_fk)
        .order_by(ExchangeItem.item_seq)
    )
    return list(db.execute(stmt).scalars().all())


def _active_item_map(db: Session, batch_fk: int) -> dict[str, ExchangeItem]:
    """每个 client_item_id 的最新非 superseded 条目。"""
    active: dict[str, ExchangeItem] = {}
    for row in _batch_items(db, batch_fk):
        if row.status == ITEM_SUPERSEDED:
            active.pop(row.client_item_id, None)
        else:
            active[row.client_item_id] = row
    return active


def _next_attempt(db: Session, batch_fk: int, client_item_id: str) -> int:
    stmt = select(func.max(ExchangeItem.attempt)).where(
        ExchangeItem.batch_fk == batch_fk,
        ExchangeItem.client_item_id == client_item_id,
    )
    current = db.execute(stmt).scalar()
    return (current or 0) + 1


def _receipt(row: ExchangeItem) -> dict[str, Any]:
    return {
        "item_seq": row.item_seq,
        "client_item_id": row.client_item_id,
        "attempt": row.attempt,
        "status": row.status,
        "fingerprint": row.fingerprint,
        "detail": row.detail,
        "retry_of": row.retry_of,
    }


def _batch_header(batch: ExchangeBatch) -> dict[str, Any]:
    return {
        "batch_id": batch.batch_id,
        "sender_id": batch.sender_id,
        "plan_version": batch.plan_version,
        "schema_version": batch.schema_version,
        "status": batch.status,
        "version": batch.version,
        "intake_fingerprint": batch.intake_fingerprint,
        "content_fingerprint": batch.content_fingerprint,
        "replay_cursor": batch.replay_cursor,
        "item_count": batch.item_count,
        "accepted_count": batch.accepted_count,
        "duplicate_count": batch.duplicate_count,
        "conflict_count": batch.conflict_count,
        "pending_count": batch.pending_count,
        "superseded_count": batch.superseded_count,
        "created_at": batch.created_at,
        "closed_at": batch.closed_at,
    }


def _bump(batch: ExchangeBatch, status: str, delta: int) -> None:
    attr = {
        ITEM_ACCEPTED: "accepted_count",
        ITEM_DUPLICATE: "duplicate_count",
        ITEM_CONFLICT: "conflict_count",
        ITEM_PENDING: "pending_count",
        ITEM_SUPERSEDED: "superseded_count",
    }[status]
    setattr(batch, attr, getattr(batch, attr) + delta)


def receive_batch(
    db: Session,
    *,
    sender_id: str,
    batch_id: str,
    plan_version: str,
    schema_version: str,
    items: list[dict[str, Any]],
) -> tuple[dict[str, Any], bool]:
    """接收批次；返回 (批次头 + 逐条回执, 是否新建)。

    相同发送方、相同批次号且接收指纹一致时幂等重放已存回执，
    内容不同则拒绝，已接受事件不会重复写入。
    """
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise UnsupportedSchemaError(
            f"schema version '{schema_version}' is not supported"
        )
    if get_plan(db, plan_version) is None:
        raise PlanNotFoundError(f"plan version '{plan_version}' is not registered")

    request_fingerprint, item_fingerprints = _fingerprint_items(plan_version, items)
    existing = _find_batch(db, sender_id=sender_id, batch_id=batch_id)
    if existing is not None:
        same_intake = (
            existing.intake_fingerprint == request_fingerprint
            and existing.plan_version == plan_version
            and existing.schema_version == schema_version
        )
        if not same_intake:
            raise BatchFingerprintMismatchError(
                f"batch '{batch_id}' already exists with a different intake fingerprint"
            )
        receipts = [
            _receipt(row)
            for row in _batch_items(db, existing.id)
            if row.status != ITEM_SUPERSEDED
        ]
        return {**_batch_header(existing), "receipts": receipts}, False

    batch = ExchangeBatch(
        batch_id=batch_id,
        sender_id=sender_id,
        plan_version=plan_version,
        schema_version=schema_version,
        status=BATCH_OPEN,
        version=1,
        intake_fingerprint=request_fingerprint,
        content_fingerprint=request_fingerprint,
        replay_cursor=0,
        item_count=0,
    )
    db.add(batch)
    db.flush()

    in_batch: dict[str, str] = {}
    attempts: dict[str, int] = {}
    accepted_ids: set[str] = set()
    receipts: list[dict[str, Any]] = []
    for item, fingerprint in zip(items, item_fingerprints):
        status, detail = _classify(
            db,
            plan_version=plan_version,
            item=item,
            fingerprint=fingerprint,
            in_batch=in_batch,
            accepted_ids=accepted_ids,
        )
        if status == ITEM_ACCEPTED:
            status, detail = _persist_accepted(
                db, plan_version=plan_version, item=item, fingerprint=fingerprint
            )
        seq = batch.replay_cursor + 1
        attempt = attempts.get(item["client_item_id"], 0) + 1
        attempts[item["client_item_id"]] = attempt
        row = ExchangeItem(
            batch_fk=batch.id,
            item_seq=seq,
            client_item_id=item["client_item_id"],
            attempt=attempt,
            event_type=item["event_type"],
            student_id=item["student_id"],
            payload=item["payload"],
            fingerprint=fingerprint,
            status=status,
            detail=detail,
            retry_of=None,
        )
        db.add(row)
        batch.replay_cursor = seq
        batch.item_count = seq
        _bump(batch, status, 1)
        in_batch[item["client_item_id"]] = fingerprint
        if status == ITEM_ACCEPTED:
            accepted_ids.add(item["client_item_id"])
        receipts.append(_receipt(row))
    batch.updated_at = _utcnow()
    db.commit()
    return {**_batch_header(batch), "receipts": receipts}, True


def get_receipts(
    db: Session,
    *,
    sender_id: str,
    batch_id: str,
    after_seq: int = 0,
    limit: int = 100,
) -> dict[str, Any]:
    """按重放游标增量拉取回执，供断点续传。"""
    batch = _find_batch(db, sender_id=sender_id, batch_id=batch_id)
    if batch is None:
        raise BatchNotFoundError(f"batch '{batch_id}' was not found for this sender")
    limit = max(1, min(limit, MAX_RECEIPT_PAGE))
    stmt = (
        select(ExchangeItem)
        .where(ExchangeItem.batch_fk == batch.id, ExchangeItem.item_seq > after_seq)
        .order_by(ExchangeItem.item_seq)
        .limit(limit)
    )
    rows = list(db.execute(stmt).scalars().all())
    next_cursor = rows[-1].item_seq if rows else after_seq
    return {
        **_batch_header(batch),
        "receipts": [_receipt(row) for row in rows],
        "next_cursor": next_cursor,
        "has_more": next_cursor < batch.replay_cursor,
    }


def supplement_batch(
    db: Session,
    *,
    sender_id: str,
    batch_id: str,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    """补交修正条目：retry_of 引用原 client_item_id，重试后原条目转为 superseded。"""
    batch = _find_batch(db, sender_id=sender_id, batch_id=batch_id)
    if batch is None:
        raise BatchNotFoundError(f"batch '{batch_id}' was not found for this sender")
    if batch.status != BATCH_OPEN:
        raise BatchClosedError(
            f"batch '{batch_id}' is sealed and no longer accepts supplements"
        )

    active = _active_item_map(db, batch.id)
    receipts: list[dict[str, Any]] = []
    for item in items:
        fingerprint = compute_item_fingerprint(
            plan_version=batch.plan_version,
            client_item_id=item["client_item_id"],
            event_type=item["event_type"],
            student_id=item["student_id"],
            payload=item["payload"],
        )
        batch.content_fingerprint = chain_fingerprint(
            batch.content_fingerprint, fingerprint
        )
        seq = batch.replay_cursor + 1
        batch.replay_cursor = seq
        batch.item_count = seq
        retry_of = (item.get("retry_of") or "").strip() or None
        target = active.get(retry_of) if retry_of else None
        attempt = _next_attempt(db, batch.id, item["client_item_id"])
        if retry_of is None:
            status, detail = (
                ITEM_CONFLICT,
                "supplement items must reference the original client_item_id via retry_of",
            )
        elif target is None:
            status, detail = (
                ITEM_CONFLICT,
                f"retry target '{retry_of}' was not found among correctable items",
            )
        elif target.status not in CORRECTABLE_STATUSES:
            status, detail = (
                ITEM_CONFLICT,
                f"retry target '{retry_of}' is '{target.status}' and cannot be corrected",
            )
        elif item["client_item_id"] != target.client_item_id:
            status, detail = (
                ITEM_CONFLICT,
                "client_item_id must match the retry target",
            )
        else:
            others = {
                cid: it.fingerprint
                for cid, it in active.items()
                if cid != target.client_item_id
            }
            accepted_ids = {
                cid for cid, it in active.items() if it.status == ITEM_ACCEPTED
            }
            status, detail = _classify(
                db,
                plan_version=batch.plan_version,
                item=item,
                fingerprint=fingerprint,
                in_batch=others,
                accepted_ids=accepted_ids,
            )
            if status == ITEM_ACCEPTED:
                status, detail = _persist_accepted(
                    db,
                    plan_version=batch.plan_version,
                    item=item,
                    fingerprint=fingerprint,
                )
            _bump(batch, target.status, -1)
            target.status = ITEM_SUPERSEDED
            target.detail = (
                f"{target.detail} | superseded by item seq {seq}"
                if target.detail
                else f"superseded by item seq {seq}"
            )
            _bump(batch, ITEM_SUPERSEDED, 1)
        row = ExchangeItem(
            batch_fk=batch.id,
            item_seq=seq,
            client_item_id=item["client_item_id"],
            attempt=attempt,
            event_type=item["event_type"],
            student_id=item["student_id"],
            payload=item["payload"],
            fingerprint=fingerprint,
            status=status,
            detail=detail,
            retry_of=retry_of,
        )
        db.add(row)
        _bump(batch, status, 1)
        if target is not None and target.status == ITEM_SUPERSEDED:
            active[target.client_item_id] = row
        receipts.append(_receipt(row))
    batch.updated_at = _utcnow()
    db.commit()
    return {**_batch_header(batch), "receipts": receipts}


def close_batch(
    db: Session, *, sender_id: str, batch_id: str, expected_version: int
) -> dict[str, Any]:
    """按预期版本号关闭批次；乐观锁保证并发关闭只有一个成功。"""
    batch = _find_batch(db, sender_id=sender_id, batch_id=batch_id)
    if batch is None:
        raise BatchNotFoundError(f"batch '{batch_id}' was not found for this sender")
    if batch.status == BATCH_SEALED:
        if batch.version == expected_version:
            return _batch_header(batch)
        raise VersionConflictError(
            f"batch '{batch_id}' is already sealed at version {batch.version}"
        )
    if batch.version != expected_version:
        raise VersionConflictError(
            f"batch '{batch_id}' is at version {batch.version}, "
            f"expected {expected_version} does not match"
        )
    now = _utcnow()
    result = db.execute(
        update(ExchangeBatch)
        .where(ExchangeBatch.id == batch.id)
        .where(ExchangeBatch.version == expected_version)
        .where(ExchangeBatch.status == BATCH_OPEN)
        .values(
            status=BATCH_SEALED,
            version=expected_version + 1,
            closed_at=now,
            updated_at=now,
        )
    )
    if result.rowcount != 1:
        db.rollback()
        raise VersionConflictError(f"batch '{batch_id}' was modified concurrently")
    db.commit()
    db.refresh(batch)
    return _batch_header(batch)


def reconcile_batch(
    db: Session,
    *,
    sender_id: str,
    batch_id: str,
    expected_fingerprint: str | None = None,
) -> dict[str, Any]:
    """对账：重算条目计数、核对事件流命中，输出平衡结论。"""
    batch = _find_batch(db, sender_id=sender_id, batch_id=batch_id)
    if batch is None:
        raise BatchNotFoundError(f"batch '{batch_id}' was not found for this sender")
    rows = _batch_items(db, batch.id)
    recount = {
        ITEM_ACCEPTED: 0,
        ITEM_DUPLICATE: 0,
        ITEM_CONFLICT: 0,
        ITEM_PENDING: 0,
        ITEM_SUPERSEDED: 0,
    }
    for row in rows:
        recount[row.status] += 1
    counts_consistent = (
        recount[ITEM_ACCEPTED] == batch.accepted_count
        and recount[ITEM_DUPLICATE] == batch.duplicate_count
        and recount[ITEM_CONFLICT] == batch.conflict_count
        and recount[ITEM_PENDING] == batch.pending_count
        and recount[ITEM_SUPERSEDED] == batch.superseded_count
        and len(rows) == batch.item_count
        and (rows[-1].item_seq if rows else 0) == batch.replay_cursor
    )
    accepted_ids = [row.client_item_id for row in rows if row.status == ITEM_ACCEPTED]
    stream_hits = 0
    if accepted_ids:
        stream_hits = db.execute(
            select(func.count(EventModel.id)).where(
                EventModel.plan_version == batch.plan_version,
                EventModel.event_id.in_(accepted_ids),
            )
        ).scalar_one()
    balanced = counts_consistent and stream_hits == batch.accepted_count
    return {
        **_batch_header(batch),
        "counts_consistent": counts_consistent,
        "event_stream_accepted": stream_hits,
        "balanced": balanced,
        "fingerprint_match": (
            expected_fingerprint == batch.content_fingerprint
            if expected_fingerprint
            else None
        ),
        "receipts": [_receipt(row) for row in rows],
    }
