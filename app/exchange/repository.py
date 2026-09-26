"""交换批次与回执的数据访问。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

from sqlalchemy import func, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from ..models import Event as EventModel
from ..models import ExchangeBatch, ExchangeReceipt


# ---------------------------------------------------------------------------
# 批次
# ---------------------------------------------------------------------------


def insert_batch(
    db: Session,
    *,
    sender_id: str,
    batch_id: str,
    plan_version: str,
    schema_version: str,
    content_fingerprint: str,
    total_count: int,
) -> ExchangeBatch | None:
    """幂等插入批次；``(sender_id, batch_id)`` 已存在时返回 None。"""

    stmt = sqlite_insert(ExchangeBatch).values(
        sender_id=sender_id,
        batch_id=batch_id,
        plan_version=plan_version,
        schema_version=schema_version,
        state="open",
        revision=1,
        content_fingerprint=content_fingerprint,
        cursor=0,
        total_count=total_count,
        accepted_count=0,
        duplicate_count=0,
        conflict_count=0,
        pending_count=0,
    )
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["sender_id", "batch_id"]
    ).returning(ExchangeBatch.id)
    batch_pk = db.execute(stmt).scalar_one_or_none()
    db.commit()
    if batch_pk is None:
        return None
    return db.get(ExchangeBatch, batch_pk)


def get_batch(
    db: Session, *, sender_id: str, batch_id: str
) -> ExchangeBatch | None:
    stmt = (
        select(ExchangeBatch)
        .where(
            ExchangeBatch.sender_id == sender_id,
            ExchangeBatch.batch_id == batch_id,
        )
        .execution_options(populate_existing=True)
    )
    return db.execute(stmt).scalar_one_or_none()


def set_cursor(db: Session, batch: ExchangeBatch, cursor: int) -> int:
    """条件推进游标：批次已关闭时命中 0 行，调用方据此回滚本行。"""

    result = db.execute(
        update(ExchangeBatch)
        .where(ExchangeBatch.id == batch.id)
        .where(ExchangeBatch.state == "open")
        .values(cursor=cursor)
    )
    if (result.rowcount or 0) > 0:
        batch.cursor = cursor
    return result.rowcount or 0


def bump_revision(db: Session, batch: ExchangeBatch) -> int | None:
    """仅当批次仍开放时原子自增版本号；已关闭返回 None。"""

    result = db.execute(
        update(ExchangeBatch)
        .where(ExchangeBatch.id == batch.id)
        .where(ExchangeBatch.state == "open")
        .values(revision=ExchangeBatch.revision + 1)
    )
    db.flush()
    db.refresh(batch)
    if (result.rowcount or 0) == 0:
        return None
    return batch.revision


def close_batch(
    db: Session, batch: ExchangeBatch, now: datetime
) -> bool:
    """条件更新：仅当批次仍为 open 时关闭，返回是否抢到关闭权。"""

    result = db.execute(
        update(ExchangeBatch)
        .where(ExchangeBatch.id == batch.id)
        .where(ExchangeBatch.state == "open")
        .values(state="closed", closed_at=now)
    )
    db.commit()
    closed = (result.rowcount or 0) > 0
    if closed:
        batch.state = "closed"
        batch.closed_at = now
    db.refresh(batch)
    return closed


# ---------------------------------------------------------------------------
# 回执
# ---------------------------------------------------------------------------


def max_line_no(db: Session, batch_pk: int) -> int:
    stmt = (
        select(ExchangeReceipt.line_no)
        .where(ExchangeReceipt.batch_pk == batch_pk)
        .order_by(ExchangeReceipt.line_no.desc())
        .limit(1)
    )
    return db.execute(stmt).scalar_one_or_none() or 0


def list_receipts(
    db: Session,
    batch_pk: int,
    *,
    after_line: int = 0,
    status: str | None = None,
    limit: int | None = None,
) -> Sequence[ExchangeReceipt]:
    stmt = select(ExchangeReceipt).where(
        ExchangeReceipt.batch_pk == batch_pk,
        ExchangeReceipt.line_no > after_line,
    )
    if status is not None:
        stmt = stmt.where(ExchangeReceipt.status == status)
    stmt = stmt.order_by(ExchangeReceipt.line_no.asc())
    if limit is not None:
        stmt = stmt.limit(limit)
    return db.execute(stmt).scalars().all()


def get_receipt(
    db: Session, batch_pk: int, line_no: int
) -> ExchangeReceipt | None:
    stmt = select(ExchangeReceipt).where(
        ExchangeReceipt.batch_pk == batch_pk,
        ExchangeReceipt.line_no == line_no,
    )
    return db.execute(stmt).scalar_one_or_none()


def receipt_event_index(
    db: Session, batch_pk: int
) -> dict[str, ExchangeReceipt]:
    """批次内 event_id -> 最早回执，用于批内冲突识别。"""

    rows = list_receipts(db, batch_pk)
    index: dict[str, ExchangeReceipt] = {}
    for row in rows:
        index.setdefault(row.event_id, row)
    return index


def insert_receipt(db: Session, **values: Any) -> ExchangeReceipt:
    row = ExchangeReceipt(**values)
    db.add(row)
    db.flush()
    return row


def mark_resolved(
    db: Session,
    receipt: ExchangeReceipt,
    *,
    resolver_line: int,
    revision: int,
) -> None:
    receipt.status = "resolved"
    receipt.resolved_by_line = resolver_line
    receipt.revision = revision


def status_counts(db: Session, batch_pk: int) -> dict[str, int]:
    rows = db.execute(
        select(ExchangeReceipt.status, func.count(ExchangeReceipt.id))
        .where(ExchangeReceipt.batch_pk == batch_pk)
        .group_by(ExchangeReceipt.status)
    ).all()
    counts = {
        "accepted": 0,
        "duplicate": 0,
        "conflict": 0,
        "pending_review": 0,
        "resolved": 0,
    }
    for status, count in rows:
        counts[status] = count
    return counts


def sync_counters(db: Session, batch: ExchangeBatch) -> dict[str, int]:
    """以回执表为准重算批次状态计数器。

    ``total_count`` 始终表示初始批条目数，不随补交行变化；游标只覆盖
    初始批，补交行通过 revision 与行号另行追踪。
    """

    counts = status_counts(db, batch.id)
    db.execute(
        update(ExchangeBatch)
        .where(ExchangeBatch.id == batch.id)
        .values(
            accepted_count=counts["accepted"],
            duplicate_count=counts["duplicate"],
            conflict_count=counts["conflict"],
            pending_count=counts["pending_review"],
        )
    )
    batch.accepted_count = counts["accepted"]
    batch.duplicate_count = counts["duplicate"]
    batch.conflict_count = counts["conflict"]
    batch.pending_count = counts["pending_review"]
    return counts


# ---------------------------------------------------------------------------
# 事件写入（与既有导入共享唯一约束，保证不重复写入）
# ---------------------------------------------------------------------------


def find_event(
    db: Session, *, event_id: str, plan_version: str
) -> EventModel | None:
    stmt = select(EventModel).where(
        EventModel.event_id == event_id,
        EventModel.plan_version == plan_version,
    )
    return db.execute(stmt).scalar_one_or_none()


def find_events(
    db: Session, *, plan_version: str, event_ids: set[str]
) -> dict[str, EventModel]:
    """按 event_id 集合批量加载事件；空集合返回空映射。"""

    if not event_ids:
        return {}
    stmt = select(EventModel).where(
        EventModel.plan_version == plan_version,
        EventModel.event_id.in_(sorted(event_ids)),
    )
    return {row.event_id: row for row in db.execute(stmt).scalars().all()}


def insert_event(
    db: Session,
    *,
    event_id: str,
    plan_version: str,
    event_type: str,
    student_id: str,
    payload: dict[str, Any],
) -> int | None:
    """插入事件；同一 ``(event_id, plan_version)`` 已存在时返回 None。"""

    stmt = sqlite_insert(EventModel).values(
        event_id=event_id,
        plan_version=plan_version,
        student_id=student_id,
        event_type=event_type,
        payload=payload,
    )
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["event_id", "plan_version"]
    ).returning(EventModel.id)
    return db.execute(stmt).scalar_one_or_none()
