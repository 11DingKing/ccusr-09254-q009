"""合作院校事件交换 API：接收、回执、补交、关闭、对账。

所有接口要求 ``X-Sender-ID`` 头标识发送方；批次按发送方归属，查询与
操作均限定在发送方自己的命名空间内。
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas import (
    ExchangeBatchIn,
    ExchangeCloseIn,
    ExchangeCloseOut,
    ExchangeReceiveOut,
    ExchangeReceiptOut,
    ExchangeReconcileOut,
    ExchangeSupplementIn,
)
from . import service

router = APIRouter(prefix="/api/plans/{plan_version}/exchange", tags=["exchange"])


def require_sender(x_sender_id: str | None = Header(default=None)) -> str:
    if not x_sender_id or not x_sender_id.strip():
        raise HTTPException(status_code=400, detail="missing X-Sender-ID header")
    return x_sender_id.strip()


def _error_status(exc: Exception) -> int:
    if isinstance(exc, service.PlanNotFoundError):
        return 404
    if isinstance(exc, service.BatchNotFoundError):
        return 404
    if isinstance(exc, service.BatchClosedError):
        return 409
    if isinstance(exc, service.BatchContentMismatchError):
        return 422
    if isinstance(exc, service.InvalidReferenceError):
        return 422
    return 400


@router.post(
    "/batches/{batch_id}",
    response_model=ExchangeReceiveOut,
    status_code=status.HTTP_201_CREATED,
)
def receive(
    plan_version: str,
    batch_id: str,
    body: ExchangeBatchIn,
    db: Session = Depends(get_db),
    sender_id: str = Depends(require_sender),
) -> Any:
    try:
        return service.receive_batch(
            db,
            sender_id=sender_id,
            plan_version=plan_version,
            batch_id=batch_id,
            events=[
                {
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "student_id": e.student_id,
                    "payload": e.payload,
                    "client_line_id": e.client_line_id,
                }
                for e in body.events
            ],
        )
    except service.ExchangeError as exc:
        raise HTTPException(status_code=_error_status(exc), detail=str(exc)) from exc


@router.get(
    "/batches/{batch_id}/receipt",
    response_model=ExchangeReceiptOut,
)
def receipt(
    plan_version: str,
    batch_id: str,
    cursor: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
    status_filter: str | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    sender_id: str = Depends(require_sender),
) -> Any:
    try:
        return service.get_receipts(
            db,
            sender_id=sender_id,
            plan_version=plan_version,
            batch_id=batch_id,
            cursor=cursor,
            limit=limit,
            status=status_filter,
        )
    except service.ExchangeError as exc:
        raise HTTPException(status_code=_error_status(exc), detail=str(exc)) from exc


@router.post(
    "/batches/{batch_id}/supplements",
    response_model=ExchangeReceiveOut,
    status_code=status.HTTP_201_CREATED,
)
def supplements(
    plan_version: str,
    batch_id: str,
    body: ExchangeSupplementIn,
    db: Session = Depends(get_db),
    sender_id: str = Depends(require_sender),
) -> Any:
    try:
        return service.submit_supplements(
            db,
            sender_id=sender_id,
            plan_version=plan_version,
            batch_id=batch_id,
            events=[
                {
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "student_id": e.student_id,
                    "payload": e.payload,
                    "client_line_id": e.client_line_id,
                    "references": e.references,
                    "resolution": e.resolution,
                }
                for e in body.events
            ],
        )
    except service.ExchangeError as exc:
        raise HTTPException(status_code=_error_status(exc), detail=str(exc)) from exc


@router.post("/batches/{batch_id}/close", response_model=ExchangeCloseOut)
def close(
    plan_version: str,
    batch_id: str,
    body: ExchangeCloseIn,
    db: Session = Depends(get_db),
    sender_id: str = Depends(require_sender),
) -> Any:
    try:
        return service.close_batch(
            db,
            sender_id=sender_id,
            plan_version=plan_version,
            batch_id=batch_id,
            force=body.force,
        )
    except service.ExchangeError as exc:
        raise HTTPException(status_code=_error_status(exc), detail=str(exc)) from exc


@router.get(
    "/batches/{batch_id}/reconcile",
    response_model=ExchangeReconcileOut,
)
def reconcile(
    plan_version: str,
    batch_id: str,
    db: Session = Depends(get_db),
    sender_id: str = Depends(require_sender),
) -> Any:
    try:
        return service.reconcile(
            db,
            sender_id=sender_id,
            plan_version=plan_version,
            batch_id=batch_id,
        )
    except service.ExchangeError as exc:
        raise HTTPException(status_code=_error_status(exc), detail=str(exc)) from exc
