"""版本化交换批次的领域服务。

处理流程全部以批次内行号为重放单位：每行回执与事件写入在同一事务内
提交，并推进 ``cursor``；进程崩溃后按相同 ``content_fingerprint`` 重新
提交即可从游标续跑，已接受事件依赖事件表唯一约束不会重复写入。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..repository import get_plan
from . import repository as repo
from .fingerprint import batch_fingerprint, payload_fingerprint
from .rules import PayloadInvalid, classify_payload
from ..models import ExchangeBatch, ExchangeReceipt

SCHEMA_VERSION = "v1"

# 测试接缝：置为整数后，每提交完该行就抛出异常模拟进程崩溃。
RESUME_FAULT_AFTER: int | None = None


class ExchangeError(Exception):
    """交换处理的基础异常。"""


class PlanNotFoundError(ExchangeError):
    pass


class BatchNotFoundError(ExchangeError):
    pass


class BatchClosedError(ExchangeError):
    pass


class BatchContentMismatchError(ExchangeError):
    pass


class InvalidReferenceError(ExchangeError):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# 输出装配
# ---------------------------------------------------------------------------


def batch_out(db: Session, batch: ExchangeBatch) -> dict[str, Any]:
    counts = repo.status_counts(db, batch.id)
    return {
        "sender_id": batch.sender_id,
        "batch_id": batch.batch_id,
        "plan_version": batch.plan_version,
        "schema_version": batch.schema_version,
        "state": batch.state,
        "revision": batch.revision,
        "content_fingerprint": batch.content_fingerprint,
        "cursor": batch.cursor,
        "total_count": batch.total_count,
        "accepted_count": counts["accepted"],
        "duplicate_count": counts["duplicate"],
        "conflict_count": counts["conflict"],
        "pending_count": counts["pending_review"],
        "resolved_count": counts["resolved"],
        "closed": batch.state == "closed",
    }


def _receipt_out(row: ExchangeReceipt) -> dict[str, Any]:
    return {
        "line_no": row.line_no,
        "revision": row.revision,
        "event_id": row.event_id,
        "event_type": row.event_type,
        "student_id": row.student_id,
        "status": row.status,
        "reason_code": row.reason_code,
        "reason_detail": row.reason_detail,
        "references_line": row.references_line,
        "resolved_by_line": row.resolved_by_line,
        "client_line_id": row.client_line_id,
        "payload_fingerprint": row.payload_fingerprint,
    }


def _require_plan(db: Session, plan_version: str) -> None:
    if get_plan(db, plan_version) is None:
        raise PlanNotFoundError(f"plan version '{plan_version}' is not registered")


def require_batch(
    db: Session, *, sender_id: str, plan_version: str, batch_id: str
) -> ExchangeBatch:
    batch = repo.get_batch(db, sender_id=sender_id, batch_id=batch_id)
    if batch is None or batch.plan_version != plan_version:
        raise BatchNotFoundError(
            f"batch '{batch_id}' not found for sender '{sender_id}'"
        )
    return batch


# ---------------------------------------------------------------------------
# 行级落库
# ---------------------------------------------------------------------------


def _classify_and_persist(
    db: Session,
    batch: ExchangeBatch,
    *,
    event_id: str,
    event_type: str,
    student_id: str,
    payload: dict[str, Any],
    fp: str,
    force_confirm: bool,
) -> tuple[str, str | None, str | None, int | None]:
    """尝试写入事件并给出回执状态，返回 (status, code, detail, event_row_id)。

    不做 commit；调用方负责与回执、游标更新一起提交。
    """

    existing = repo.find_event(
        db, event_id=event_id, plan_version=batch.plan_version
    )
    if existing is not None:
        existing_fp = payload_fingerprint(existing.payload)
        if existing_fp == fp:
            return "duplicate", "already_accepted", None, existing.id
        return (
            "conflict",
            "event_content_mismatch",
            f"event '{event_id}' already stored with different content",
            None,
        )

    verdict, code, detail = ("accept", None, None)
    try:
        verdict, code, detail = classify_payload(event_type, payload)
    except PayloadInvalid as exc:
        return "conflict", "payload_invalid", str(exc), None

    if force_confirm:
        verdict, code, detail = "accept", "review_confirmed", detail

    if verdict == "review":
        return "pending_review", code, detail, None

    row_id = repo.insert_event(
        db,
        event_id=event_id,
        plan_version=batch.plan_version,
        event_type=event_type,
        student_id=student_id,
        payload=payload,
    )
    if row_id is not None:
        return "accepted", code, detail, row_id

    # 并发写入：其他事务抢先落库，重新比较内容。
    raced = repo.find_event(db, event_id=event_id, plan_version=batch.plan_version)
    assert raced is not None
    if payload_fingerprint(raced.payload) == fp:
        return "duplicate", "already_accepted", None, raced.id
    return (
        "conflict",
        "event_content_mismatch",
        f"event '{event_id}' already stored with different content",
        None,
    )


def _intra_batch_status(
    index: dict[str, ExchangeReceipt], event_id: str, fp: str
) -> tuple[str, str, str] | None:
    prior = index.get(event_id)
    if prior is None:
        return None
    if prior.payload_fingerprint == fp and prior.status in {
        "accepted",
        "duplicate",
    }:
        return (
            "duplicate",
            "duplicate_in_batch",
            f"event '{event_id}' duplicates line {prior.line_no}",
        )
    return (
        "conflict",
        "duplicate_event_id_in_batch",
        f"event '{event_id}' was already submitted at line {prior.line_no}",
    )


# ---------------------------------------------------------------------------
# 接收（含重放与重启恢复）
# ---------------------------------------------------------------------------


def receive_batch(
    db: Session,
    *,
    sender_id: str,
    plan_version: str,
    batch_id: str,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    _require_plan(db, plan_version)
    fingerprint = batch_fingerprint(events)
    batch = repo.insert_batch(
        db,
        sender_id=sender_id,
        batch_id=batch_id,
        plan_version=plan_version,
        schema_version=SCHEMA_VERSION,
        content_fingerprint=fingerprint,
        total_count=len(events),
    )

    if batch is None:
        batch = require_batch(
            db, sender_id=sender_id, plan_version=plan_version, batch_id=batch_id
        )
        if batch.state == "closed":
            raise BatchClosedError(f"batch '{batch_id}' is closed")
        if batch.content_fingerprint != fingerprint:
            raise BatchContentMismatchError(
                "batch_id reused with different content; checksum verification failed"
            )
        resumed = _resume_pending(db, batch, events)
        repo.sync_counters(db, batch)
        db.commit()
        return _receive_summary(db, batch, replay=True, resumed=resumed)

    resumed = _ingest_lines(db, batch, events, start_line=1)
    repo.sync_counters(db, batch)
    db.commit()
    return _receive_summary(db, batch, replay=False, resumed=resumed)


def _resume_pending(
    db: Session, batch: ExchangeBatch, events: Sequence[dict[str, Any]]
) -> bool:
    """崩溃恢复：补处理游标之后或回执缺失的行。"""

    resumed = False
    index = repo.receipt_event_index(db, batch.id)
    for line_no in range(batch.cursor + 1, batch.total_count + 1):
        if repo.get_receipt(db, batch.id, line_no) is not None:
            # 回执已提交而游标落后：只推进游标，绝不重复处理。
            if repo.set_cursor(db, batch, line_no) == 0:
                raise BatchClosedError(f"batch '{batch.batch_id}' is closed")
            db.commit()
            resumed = True
            continue
        item = events[line_no - 1]
        _ingest_one(db, batch, item, line_no=line_no, revision=1, index=index)
        resumed = True
    return resumed


def _ingest_lines(
    db: Session,
    batch: ExchangeBatch,
    events: Sequence[dict[str, Any]],
    *,
    start_line: int,
) -> bool:
    index: dict[str, ExchangeReceipt] = {}
    for offset, item in enumerate(events):
        line_no = start_line + offset
        _ingest_one(db, batch, item, line_no=line_no, revision=1, index=index)
    return start_line > 1


def _ingest_one(
    db: Session,
    batch: ExchangeBatch,
    item: dict[str, Any],
    *,
    line_no: int,
    revision: int,
    index: dict[str, ExchangeReceipt],
) -> ExchangeReceipt:
    try:
        return _ingest_one_unchecked(
            db, batch, item, line_no=line_no, revision=revision, index=index
        )
    except IntegrityError:
        # 并发首次提交同一批次：对方已写入本行回执与事件，回滚本行
        # 未提交的工作，确认对方结果后推进游标。
        db.rollback()
        existing = repo.get_receipt(db, batch.id, line_no)
        if existing is None:
            raise
        # 对方已完成本行（事件是否落库以其回执为准），对齐游标即可。
        if repo.set_cursor(db, batch, line_no) == 0:
            db.rollback()
            raise BatchClosedError(f"batch '{batch.batch_id}' is closed")
        db.commit()
        index.setdefault(existing.event_id, existing)
        return existing


def _ingest_one_unchecked(
    db: Session,
    batch: ExchangeBatch,
    item: dict[str, Any],
    *,
    line_no: int,
    revision: int,
    index: dict[str, ExchangeReceipt],
) -> ExchangeReceipt:
    fp = payload_fingerprint(item["payload"])
    intra = _intra_batch_status(index, item["event_id"], fp)
    if intra is not None:
        status, code, detail = intra
        receipt = repo.insert_receipt(
            db,
            batch_pk=batch.id,
            line_no=line_no,
            revision=revision,
            event_id=item["event_id"],
            event_type=item["event_type"],
            student_id=item["student_id"],
            payload=item["payload"],
            payload_fingerprint=fp,
            client_line_id=item.get("client_line_id", ""),
            status=status,
            reason_code=code,
            reason_detail=detail,
        )
    else:
        status, code, detail, row_id = _classify_and_persist(
            db,
            batch,
            event_id=item["event_id"],
            event_type=item["event_type"],
            student_id=item["student_id"],
            payload=item["payload"],
            fp=fp,
            force_confirm=False,
        )
        receipt = repo.insert_receipt(
            db,
            batch_pk=batch.id,
            line_no=line_no,
            revision=revision,
            event_id=item["event_id"],
            event_type=item["event_type"],
            student_id=item["student_id"],
            payload=item["payload"],
            payload_fingerprint=fp,
            client_line_id=item.get("client_line_id", ""),
            status=status,
            reason_code=code,
            reason_detail=detail,
            event_row_id=row_id,
        )
    index.setdefault(receipt.event_id, receipt)
    affected = repo.set_cursor(db, batch, line_no)
    if affected == 0:
        # 关闭事务在本行处理期间抢先关闭：撤销本行写入。
        db.rollback()
        raise BatchClosedError(f"batch '{batch.batch_id}' was closed during ingest")
    db.commit()
    global RESUME_FAULT_AFTER
    if RESUME_FAULT_AFTER is not None and line_no >= RESUME_FAULT_AFTER:
        RESUME_FAULT_AFTER = None
        raise RuntimeError("simulated crash after committed line")
    return receipt


def _receive_summary(
    db: Session, batch: ExchangeBatch, *, replay: bool, resumed: bool
) -> dict[str, Any]:
    rows = repo.list_receipts(db, batch.id)
    accepted, duplicates, conflicts, pending = [], [], [], []
    for row in rows:
        bucket = {
            "accepted": accepted,
            "duplicate": duplicates,
            "conflict": conflicts,
            "pending_review": pending,
        }.get(row.status)
        if bucket is None:
            continue
        if row.status == "accepted":
            bucket.append({"line_no": row.line_no, "event_id": row.event_id})
        else:
            bucket.append(row.line_no)
    return {
        "batch": batch_out(db, batch),
        "replay": replay,
        "resumed": resumed,
        "accepted": accepted,
        "duplicates": duplicates,
        "conflicts": conflicts,
        "pending_review": pending,
    }


# ---------------------------------------------------------------------------
# 回执查询（重放游标分页）
# ---------------------------------------------------------------------------


def get_receipts(
    db: Session,
    *,
    sender_id: str,
    plan_version: str,
    batch_id: str,
    cursor: int,
    limit: int,
    status: str | None,
) -> dict[str, Any]:
    batch = require_batch(
        db, sender_id=sender_id, plan_version=plan_version, batch_id=batch_id
    )
    rows = repo.list_receipts(
        db, batch.id, after_line=cursor, status=status, limit=limit
    )
    items = [_receipt_out(r) for r in rows]
    next_cursor: int | None = None
    if rows:
        last_line = rows[-1].line_no
        if repo.list_receipts(db, batch.id, after_line=last_line, limit=1):
            next_cursor = last_line
    return {
        "batch": batch_out(db, batch),
        "items": items,
        "next_cursor": next_cursor,
    }


# ---------------------------------------------------------------------------
# 补交重试
# ---------------------------------------------------------------------------


def submit_supplements(
    db: Session,
    *,
    sender_id: str,
    plan_version: str,
    batch_id: str,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    batch = require_batch(
        db, sender_id=sender_id, plan_version=plan_version, batch_id=batch_id
    )
    if not events:
        return _receive_summary(db, batch, replay=True, resumed=False)
    if batch.state == "closed":
        raise BatchClosedError(f"batch '{batch_id}' is closed")

    # 引用预检：所有引用必须存在、仍可重试、事件标识一致，且同一目标
    # 行不能在同一次补交中被多次引用。
    targets: dict[int, ExchangeReceipt] = {}
    seen_refs: set[int] = set()
    for item in events:
        ref = item.get("references")
        if ref is None:
            continue
        if ref in seen_refs:
            raise InvalidReferenceError(
                f"line {ref} is referenced more than once in this supplement"
            )
        seen_refs.add(ref)
        target = repo.get_receipt(db, batch.id, ref)
        if target is None:
            raise InvalidReferenceError(f"references line {ref} does not exist")
        if target.status not in {"conflict", "pending_review"}:
            raise InvalidReferenceError(
                f"line {ref} is '{target.status}', only conflict/pending_review "
                "items can be retried"
            )
        if target.event_id != item["event_id"]:
            raise InvalidReferenceError(
                f"line {ref} correction must keep event_id '{target.event_id}'"
            )
        targets[ref] = target

    # 条件自增修订号即对批次行加写锁：关闭事务必须等本事务结束，
    # 因此整个补交过程与关闭严格互斥；批次已关闭时返回 None。
    new_revision = repo.bump_revision(db, batch)
    if new_revision is None:
        db.rollback()
        raise BatchClosedError(f"batch '{batch_id}' is closed")

    next_line = repo.max_line_no(db, batch.id)

    for item in events:
        next_line += 1
        target = targets.get(item["references"]) if item.get("references") else None
        _supplement_one(
            db,
            batch,
            item,
            line_no=next_line,
            revision=new_revision,
            target=target,
        )

    repo.sync_counters(db, batch)
    db.commit()
    return _receive_summary(db, batch, replay=False, resumed=False)


def _supplement_one(
    db: Session,
    batch: ExchangeBatch,
    item: dict[str, Any],
    *,
    line_no: int,
    revision: int,
    target: ExchangeReceipt | None,
) -> None:
    fp = payload_fingerprint(item["payload"])
    force_confirm = target is not None and item.get("resolution") == "confirm"
    status, code, detail, row_id = _classify_and_persist(
        db,
        batch,
        event_id=item["event_id"],
        event_type=item["event_type"],
        student_id=item["student_id"],
        payload=item["payload"],
        fp=fp,
        force_confirm=force_confirm,
    )

    repo.insert_receipt(
        db,
        batch_pk=batch.id,
        line_no=line_no,
        revision=revision,
        event_id=item["event_id"],
        event_type=item["event_type"],
        student_id=item["student_id"],
        payload=item["payload"],
        payload_fingerprint=fp,
        client_line_id=item.get("client_line_id", ""),
        status=status,
        reason_code=code,
        reason_detail=detail,
        references_line=target.line_no if target is not None else None,
        event_row_id=row_id,
    )
    # 仅当补交真正落地（接受或重复）时，原冲突/待审核条目才翻转为 resolved；
    # 补交仍是冲突/待审核时保留目标条目原状，新行记录本次失败尝试。
    if target is not None and status in {"accepted", "duplicate"}:
        repo.mark_resolved(db, target, resolver_line=line_no, revision=revision)


# ---------------------------------------------------------------------------
# 关闭
# ---------------------------------------------------------------------------


def close_batch(
    db: Session,
    *,
    sender_id: str,
    plan_version: str,
    batch_id: str,
    force: bool,
) -> dict[str, Any]:
    batch = require_batch(
        db, sender_id=sender_id, plan_version=plan_version, batch_id=batch_id
    )
    counts = repo.status_counts(db, batch.id)
    unresolved_conflicts = counts["conflict"]
    unresolved_pending = counts["pending_review"]

    if batch.state == "closed":
        return {
            "batch": batch_out(db, batch),
            "closed": False,
            "unresolved_conflicts": unresolved_conflicts,
            "unresolved_pending": unresolved_pending,
        }

    if not force and (unresolved_conflicts or unresolved_pending):
        raise BatchClosedError(
            f"batch has {unresolved_conflicts} conflict(s) and "
            f"{unresolved_pending} pending review; resolve or use force"
        )

    closed = repo.close_batch(db, batch, now=_utcnow())
    counts = repo.status_counts(db, batch.id)
    return {
        "batch": batch_out(db, batch),
        "closed": closed,
        "unresolved_conflicts": counts["conflict"],
        "unresolved_pending": counts["pending_review"],
    }


# ---------------------------------------------------------------------------
# 对账
# ---------------------------------------------------------------------------


def reconcile(
    db: Session, *, sender_id: str, plan_version: str, batch_id: str
) -> dict[str, Any]:
    batch = require_batch(
        db, sender_id=sender_id, plan_version=plan_version, batch_id=batch_id
    )
    rows = list(repo.list_receipts(db, batch.id))
    by_line = {row.line_no: row for row in rows}

    # 一次性加载本批次涉及的全部事件，避免逐行查询。
    event_ids = {r.event_id for r in rows}
    stored_events = repo.find_events(
        db, plan_version=batch.plan_version, event_ids=event_ids
    )

    # 整批指纹：取初始提交（无引用）行，按行号重算。
    original = [r for r in rows if r.references_line is None]
    original_material = [
        {
            "event_id": r.event_id,
            "event_type": r.event_type,
            "student_id": r.student_id,
            "payload": r.payload,
        }
        for r in sorted(original, key=lambda r: r.line_no)
    ]
    fingerprint_ok = (
        batch_fingerprint(original_material) == batch.content_fingerprint
    )
    cursor_complete = batch.cursor >= len(original_material) and len(
        original_material
    ) == batch.total_count

    items: list[dict[str, Any]] = []
    issues = 0
    event_rows: set[int] = set()
    expected_event_ids: set[str] = set()

    for row in rows:
        issue: str | None = None
        event = stored_events.get(row.event_id)

        if row.status == "accepted":
            expected_event_ids.add(row.event_id)
            if row.event_row_id is None:
                issue = "event_missing"
            elif event is None:
                issue = "event_row_dangling"
            elif event.id != row.event_row_id:
                issue = "event_row_drift"
            elif payload_fingerprint(event.payload) != row.payload_fingerprint:
                issue = "content_drift"
            else:
                event_rows.add(event.id)

        elif row.status == "duplicate":
            expected_event_ids.add(row.event_id)
            if event is None:
                issue = "duplicate_without_event"
            elif payload_fingerprint(event.payload) != row.payload_fingerprint:
                issue = "duplicate_content_drift"
            else:
                event_rows.add(event.id)

        elif row.status == "pending_review":
            if event is not None and payload_fingerprint(event.payload) == row.payload_fingerprint:
                issue = "pending_already_written"
            else:
                issue = "unresolved_pending"

        elif row.status == "conflict":
            issue = "unresolved_conflict"

        elif row.status == "resolved":
            resolver = by_line.get(row.resolved_by_line or -1)
            if resolver is None or resolver.status not in {"accepted", "duplicate"}:
                issue = "resolution_broken"

        if issue is not None:
            issues += 1
        items.append(
            {
                "line_no": row.line_no,
                "status": row.status,
                "event_id": row.event_id,
                "issue": issue,
                "event_row_id": row.event_row_id,
            }
        )

    return {
        "batch": batch_out(db, batch),
        "fingerprint_ok": fingerprint_ok,
        "cursor_complete": cursor_complete,
        "events_expected": len(expected_event_ids),
        "events_found": len(event_rows),
        "balanced": fingerprint_ok and cursor_complete and issues == 0,
        "items": items,
    }
