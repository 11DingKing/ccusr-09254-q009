"""交换内容的规范化与指纹计算。"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence

Canonical = dict[str, Any] | list[Any] | str | int | float | bool | None


def canonical_json(value: Canonical) -> str:
    """对 JSON 兼容数据做确定性序列化。

    键排序、紧凑分隔符、保留非 ASCII 字符，使同一语义内容的不同排版
    （空白、键顺序）产生相同字符串。
    """

    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def payload_fingerprint(payload: Mapping[str, Any]) -> str:
    return sha256_hex(canonical_json(dict(payload)))


def batch_fingerprint(events: Sequence[Mapping[str, Any]]) -> str:
    """整批指纹：按提交顺序规范化全部条目。

    指纹覆盖事件标识、类型、学员与原始载荷，顺序不同也视为不同批次。
    """

    material = [
        {
            "event_id": e["event_id"],
            "event_type": e["event_type"],
            "student_id": e["student_id"],
            "payload": e["payload"],
        }
        for e in events
    ]
    return sha256_hex(canonical_json(material))
