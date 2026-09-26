"""外部事件交换 API：接收、回执、补交、关闭与对账。"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from sqlalchemy.orm import Session

from ..db import get_db
from . import service as exchange_service
from .schemas import (
    BatchHeaderOut,
    BatchReceiveOut,
    CloseBatchIn,
    ReceiptsPageOut,
    ReceiveBatchIn,
    ReconciliationOut,
    SupplementBatchIn,
    SupplementOut,
)

router = APIRouter(prefix="/api/exchange", tags=["exchange"])


def _sender_id(x_sender_id: str = Header(..., alias="X-Sender-Id")) -> str:
    """发送方身份；缺失或空白时拒绝，跨发送方访问一律按不存在处理。"""
    sender = x_sender_id.strip()
    if not sender:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="X-Sender-Id header must not be blank",
        )
    return sender


@router.post(
    "/batches",
    response_model=BatchReceiveOut,
    status_code=status.HTTP_201_CREATED,
)
def receive_batch(
    body: ReceiveBatchIn,
    response: Response,
    sender_id: str = Depends(_sender_id),
    db: Session = Depends(get_db),
) -> Any:
    try:
        payload, created = exchange_service.receive_batch(
            db,
            sender_id=sender_id,
            batch_id=body.batch_id,
            plan_version=body.plan_version,
            schema_version=body.schema_version,
            items=[item.model_dump() for item in body.items],
        )
    except exchange_service.UnsupportedSchemaError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except exchange_service.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except exchange_service.BatchFingerprintMismatchError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if not created:
        response.status_code = status.HTTP_200_OK
    return payload


@router.get("/batches/{batch_id}/receipts", response_model=ReceiptsPageOut)
def get_receipts(
    batch_id: str,
    after_seq: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    sender_id: str = Depends(_sender_id),
    db: Session = Depends(get_db),
) -> Any:
    try:
        return exchange_service.get_receipts(
            db,
            sender_id=sender_id,
            batch_id=batch_id,
            after_seq=after_seq,
            limit=limit,
        )
    except exchange_service.BatchNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/batches/{batch_id}/supplements",
    response_model=SupplementOut,
    status_code=status.HTTP_201_CREATED,
)
def supplement_batch(
    batch_id: str,
    body: SupplementBatchIn,
    sender_id: str = Depends(_sender_id),
    db: Session = Depends(get_db),
) -> Any:
    try:
        return exchange_service.supplement_batch(
            db,
            sender_id=sender_id,
            batch_id=batch_id,
            items=[item.model_dump() for item in body.items],
        )
    except exchange_service.BatchNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except exchange_service.BatchClosedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/batches/{batch_id}/close", response_model=BatchHeaderOut)
def close_batch(
    batch_id: str,
    body: CloseBatchIn,
    sender_id: str = Depends(_sender_id),
    db: Session = Depends(get_db),
) -> Any:
    try:
        return exchange_service.close_batch(
            db,
            sender_id=sender_id,
            batch_id=batch_id,
            expected_version=body.expected_version,
        )
    except exchange_service.BatchNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except exchange_service.VersionConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/batches/{batch_id}/reconciliation", response_model=ReconciliationOut)
def reconcile_batch(
    batch_id: str,
    fingerprint: str | None = Query(default=None, min_length=1, max_length=64),
    sender_id: str = Depends(_sender_id),
    db: Session = Depends(get_db),
) -> Any:
    try:
        return exchange_service.reconcile_batch(
            db,
            sender_id=sender_id,
            batch_id=batch_id,
            expected_fingerprint=fingerprint,
        )
    except exchange_service.BatchNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
