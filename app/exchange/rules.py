"""交换条目的载荷校验与待审核规则。

校验失败属于确定性冲突（reason_code=payload_invalid），发送方修正后可
补交重试；校验通过但命中审核规则的条目标记为 pending_review，等待补交
确认或平台侧后续处理。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import ValidationError

from ..schemas import CheckinPayload, LeaveCorrectionPayload, MentorConfirmPayload

Verdict = Literal["accept", "review"]

# 单次签到超过 24 小时视为异常时长，需要人工审核。
MAX_CHECKIN_SECONDS = 24 * 3600


def classify_payload(
    event_type: str, payload: dict[str, Any]
) -> tuple[Verdict, str | None, str | None]:
    """返回 (判定, reason_code, reason_detail)。

    判定为 ``accept`` 时可直接写入事件；为 ``review`` 时进入待审核；
    载荷本身不合法时抛出 :class:`PayloadInvalid`（由调用方记为冲突）。
    """

    try:
        if event_type == "checkin":
            parsed = CheckinPayload.model_validate(payload)
        elif event_type == "mentor_confirm":
            parsed = MentorConfirmPayload.model_validate(payload)
        elif event_type == "leave_correction":
            parsed = LeaveCorrectionPayload.model_validate(payload)
        else:
            raise PayloadInvalid(f"unknown event_type '{event_type}'")
    except PayloadInvalid:
        raise
    except ValidationError as exc:
        raise PayloadInvalid(_first_error(exc)) from exc

    if event_type == "checkin":
        seconds = int(
            (parsed.check_out_at - parsed.check_in_at).total_seconds()
        )
        if seconds > MAX_CHECKIN_SECONDS:
            return (
                "review",
                "checkin_too_long",
                f"checkin spans {seconds}s, exceeds {MAX_CHECKIN_SECONDS}s",
            )
    if event_type == "leave_correction":
        if abs(parsed.adjustment_seconds) > MAX_CHECKIN_SECONDS:
            return (
                "review",
                "adjustment_out_of_range",
                f"adjustment_seconds={parsed.adjustment_seconds} requires review",
            )
    return "accept", None, None


class PayloadInvalid(Exception):
    """载荷无法通过事件类型对应的结构校验。"""


def _first_error(exc: ValidationError) -> str:
    first = exc.errors()[0]
    loc = ".".join(str(p) for p in first.get("loc", ()))
    return f"{loc}: {first.get('msg', 'invalid payload')}"


def parse_dt(value: Any) -> datetime:
    """供测试与对账复用的严格时间解析。"""
    if not isinstance(value, str):
        raise PayloadInvalid("timestamp must be an RFC 3339 string")
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise PayloadInvalid("timestamp must be timezone-aware")
    return dt
