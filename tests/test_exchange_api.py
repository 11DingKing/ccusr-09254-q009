"""交换批次 API 与重启恢复测试。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import threading

import pytest

from app.exchange import service as exchange_service
from app.models import Event as EventModel
from tests.conftest import SHANGHAI_PLAN, TestSessionLocal

PLAN = SHANGHAI_PLAN["plan_version"]
SENDER = "school-alpha"
HEADERS = {"X-Sender-ID": SENDER}


def _create_plan(client):
    resp = client.post("/api/plans", json=SHANGHAI_PLAN)
    assert resp.status_code == 201, resp.text


def _checkin(event_id: str, student: str, *, hours: int = 2, start_day: int = 1):
    start = datetime(2024, 3, start_day, 8, tzinfo=timezone(timedelta(hours=8)))
    end = start + timedelta(hours=hours)
    return {
        "event_id": event_id,
        "event_type": "checkin",
        "student_id": student,
        "payload": {
            "activity_id": f"A-{event_id}",
            "activity_type": "regular",
            "check_in_at": start.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
            "check_out_at": end.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
        },
    }


def _receive(client, batch_id: str, events, sender: str = SENDER):
    resp = client.post(
        f"/api/plans/{PLAN}/exchange/batches/{batch_id}",
        json={"batch_id": batch_id, "events": events},
        headers={"X-Sender-ID": sender},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# 基础接收与四态回执
# ---------------------------------------------------------------------------


def test_receive_classifies_all_four_statuses(client):
    _create_plan(client)
    events = [
        _checkin("E-OK", "S1"),  # accepted
        _checkin("E-OK", "S1"),  # 批内同内容重复 -> duplicate
        {  # 同 ID 不同内容 -> 批内冲突
            "event_id": "E-OK",
            "event_type": "checkin",
            "student_id": "S2",
            "payload": {
                "activity_id": "A-other",
                "activity_type": "regular",
                "check_in_at": "2024-03-02T08:00:00+08:00",
                "check_out_at": "2024-03-02T10:00:00+08:00",
            },
        },
        {  # 载荷非法 -> 冲突
            "event_id": "E-BAD",
            "event_type": "checkin",
            "student_id": "S1",
            "payload": {
                "check_in_at": "2024-03-03T10:00:00+08:00",
                "check_out_at": "2024-03-03T08:00:00+08:00",
            },
        },
        _checkin("E-LONG", "S1", hours=25),  # 超长签到 -> 待审核
    ]
    body = _receive(client, "B-1", events)
    batch = body["batch"]
    assert batch["total_count"] == 5
    assert batch["accepted_count"] == 1
    assert batch["duplicate_count"] == 1
    assert batch["conflict_count"] == 2
    assert batch["pending_count"] == 1
    assert batch["revision"] == 1
    assert len(batch["content_fingerprint"]) == 64
    assert body["accepted"] == [{"line_no": 1, "event_id": "E-OK"}]
    assert body["duplicates"] == [2]
    assert body["conflicts"] == [3, 4]
    assert body["pending_review"] == [5]

    receipt = client.get(
        f"/api/plans/{PLAN}/exchange/batches/B-1/receipt",
        headers=HEADERS,
    ).json()
    by_line = {item["line_no"]: item for item in receipt["items"]}
    assert by_line[3]["reason_code"] == "duplicate_event_id_in_batch"
    assert by_line[4]["reason_code"] == "payload_invalid"
    assert by_line[5]["reason_code"] == "checkin_too_long"


def test_sender_header_required_and_isolation(client):
    _create_plan(client)
    _receive(client, "B-ISO", [_checkin("E-1", "S1")])

    # 缺少发送方头 -> 400
    resp = client.get(f"/api/plans/{PLAN}/exchange/batches/B-ISO/receipt")
    assert resp.status_code == 400

    # 其他发送方看不到该批次 -> 404
    resp = client.get(
        f"/api/plans/{PLAN}/exchange/batches/B-ISO/receipt",
        headers={"X-Sender-ID": "school-beta"},
    )
    assert resp.status_code == 404

    # 同名批次对另一发送方是独立资源
    other = _receive(
        client, "B-ISO", [_checkin("E-2", "S9")], sender="school-beta"
    )
    assert other["batch"]["sender_id"] == "school-beta"
    assert other["batch"]["accepted_count"] == 1


# ---------------------------------------------------------------------------
# 大批量重放：已接受项不得重复写入
# ---------------------------------------------------------------------------


def test_large_batch_replay_is_idempotent(client):
    _create_plan(client)
    n = 300
    events = [
        _checkin(f"E-{i:04d}", f"S{i % 13}", hours=1, start_day=(i % 27) + 1)
        for i in range(n)
    ]
    body = _receive(client, "B-BIG", events)
    assert body["batch"]["accepted_count"] == n

    def _event_count():
        session = TestSessionLocal()
        try:
            return (
                session.query(EventModel)
                .filter(EventModel.plan_version == PLAN)
                .count()
            )
        finally:
            session.close()

    assert _event_count() == n

    # 用完全相同的内容重放整批：回执保持已接受、零新增写入、游标已满。
    replay = _receive(client, "B-BIG", events)
    assert replay["replay"] is True
    assert replay["resumed"] is False
    assert replay["batch"]["accepted_count"] == n
    assert replay["batch"]["duplicate_count"] == 0
    assert replay["batch"]["cursor"] == n
    assert _event_count() == n

    # 另一批次提交相同 event_id：逐条识别为重复，仍不重复写入。
    cross = _receive(client, "B-BIG-COPY", events)
    assert cross["batch"]["accepted_count"] == 0
    assert cross["batch"]["duplicate_count"] == n
    assert _event_count() == n

    # 内容指纹不一致 -> 拒绝（防止把别的内容塞进同一批次）
    tampered = [dict(e) for e in events]
    tampered[0] = _checkin("E-0000", "S1", hours=2)
    resp = client.post(
        f"/api/plans/{PLAN}/exchange/batches/B-BIG",
        json={"batch_id": "B-BIG", "events": tampered},
        headers=HEADERS,
    )
    assert resp.status_code == 422


def test_receipt_cursor_pagination(client):
    _create_plan(client)
    events = [_checkin(f"E-{i:03d}", "S1", hours=1) for i in range(5)]
    _receive(client, "B-PAGE", events)

    page1 = client.get(
        f"/api/plans/{PLAN}/exchange/batches/B-PAGE/receipt?cursor=0&limit=2",
        headers=HEADERS,
    ).json()
    assert [i["line_no"] for i in page1["items"]] == [1, 2]
    assert page1["next_cursor"] == 2

    page2 = client.get(
        f"/api/plans/{PLAN}/exchange/batches/B-PAGE/receipt?cursor=2&limit=2",
        headers=HEADERS,
    ).json()
    assert [i["line_no"] for i in page2["items"]] == [3, 4]
    assert page2["next_cursor"] == 4

    page3 = client.get(
        f"/api/plans/{PLAN}/exchange/batches/B-PAGE/receipt?cursor=4&limit=2",
        headers=HEADERS,
    ).json()
    assert [i["line_no"] for i in page3["items"]] == [5]
    assert page3["next_cursor"] is None


# ---------------------------------------------------------------------------
# 补交：冲突修正引用原条目重试
# ---------------------------------------------------------------------------


def test_supplement_resolves_conflict_and_pending(client, db):
    _create_plan(client)
    events = [
        {
            "event_id": "E-BAD",
            "event_type": "checkin",
            "student_id": "S1",
            "payload": {
                "check_in_at": "2024-03-03T10:00:00+08:00",
                "check_out_at": "2024-03-03T08:00:00+08:00",
            },
        },
        _checkin("E-LONG", "S1", hours=25),
    ]
    body = _receive(client, "B-FIX", events)
    assert body["conflicts"] == [1]
    assert body["pending_review"] == [2]

    # 修正后的载荷引用第 1 行；待审核项以 confirm 引用第 2 行。
    resp = client.post(
        f"/api/plans/{PLAN}/exchange/batches/B-FIX/supplements",
        json={
            "events": [
                {
                    "event_id": "E-BAD",
                    "event_type": "checkin",
                    "student_id": "S1",
                    "references": 1,
                    "payload": {
                        "activity_id": "A1",
                        "activity_type": "regular",
                        "check_in_at": "2024-03-03T08:00:00+08:00",
                        "check_out_at": "2024-03-03T10:00:00+08:00",
                    },
                },
                {
                    "event_id": "E-LONG",
                    "event_type": "checkin",
                    "student_id": "S1",
                    "references": 2,
                    "resolution": "confirm",
                    "payload": events[1]["payload"],
                },
            ]
        },
        headers=HEADERS,
    )
    assert resp.status_code == 201, resp.text
    result = resp.json()
    assert result["batch"]["revision"] == 2
    assert result["batch"]["accepted_count"] == 2
    assert result["batch"]["conflict_count"] == 0
    assert result["batch"]["pending_count"] == 0
    assert result["batch"]["resolved_count"] == 2
    assert {a["event_id"] for a in result["accepted"]} == {"E-BAD", "E-LONG"}

    receipt = client.get(
        f"/api/plans/{PLAN}/exchange/batches/B-FIX/receipt",
        headers=HEADERS,
    ).json()
    by_line = {i["line_no"]: i for i in receipt["items"]}
    assert by_line[1]["status"] == "resolved"
    assert by_line[1]["resolved_by_line"] == 3
    assert by_line[2]["status"] == "resolved"
    assert by_line[2]["resolved_by_line"] == 4
    assert by_line[3]["references_line"] == 1
    assert by_line[3]["status"] == "accepted"

    # 事件表每个 event_id 恰好一行。
    rows = (
        db.query(EventModel)
        .filter(EventModel.plan_version == PLAN)
        .all()
    )
    ids = [r.event_id for r in rows]
    assert sorted(ids) == ["E-BAD", "E-LONG"]


def test_supplement_validation_rules(client):
    _create_plan(client)
    _receive(client, "B-RULES", [_checkin("E-OK", "S1"), _checkin("E-LONG", "S1", hours=25)])

    def _post(payload, expected: int):
        resp = client.post(
            f"/api/plans/{PLAN}/exchange/batches/B-RULES/supplements",
            json={"events": payload},
            headers=HEADERS,
        )
        assert resp.status_code == expected, resp.text
        return resp

    # 引用不存在的行
    _post(
        [{"event_id": "E-X", "event_type": "checkin", "student_id": "S1",
          "references": 99, "payload": _checkin("E-X", "S1")["payload"]}],
        422,
    )
    # 已接受行不允许重试
    _post(
        [{"event_id": "E-OK", "event_type": "checkin", "student_id": "S1",
          "references": 1, "payload": _checkin("E-OK", "S1")["payload"]}],
        422,
    )
    # 修正时不得更换 event_id
    _post(
        [{"event_id": "E-OTHER", "event_type": "checkin", "student_id": "S1",
          "references": 2, "resolution": "confirm",
          "payload": _checkin("E-LONG", "S1", hours=25)["payload"]}],
        422,
    )

    # 修正失败（仍是非法载荷）：目标行保持 pending，新行记录失败尝试
    resp = _post(
        [{
            "event_id": "E-LONG", "event_type": "checkin", "student_id": "S1",
            "references": 2,
            "payload": {
                "check_in_at": "2024-03-03T10:00:00+08:00",
                "check_out_at": "2024-03-03T08:00:00+08:00",
            },
        }],
        201,
    )
    body = resp.json()
    assert body["batch"]["pending_count"] == 1
    receipt = client.get(
        f"/api/plans/{PLAN}/exchange/batches/B-RULES/receipt",
        headers=HEADERS,
    ).json()
    by_line = {i["line_no"]: i for i in receipt["items"]}
    assert by_line[2]["status"] == "pending_review"
    assert by_line[2]["resolved_by_line"] is None
    assert by_line[3]["status"] == "conflict"
    assert by_line[3]["references_line"] == 2


# ---------------------------------------------------------------------------
# 关闭（含并发）
# ---------------------------------------------------------------------------


def test_close_requires_resolution_then_force_and_blocks_supplements(client):
    _create_plan(client)
    _receive(client, "B-CLOSE", [_checkin("E-LONG", "S1", hours=25)])

    # 存在待审核项，普通关闭被拒
    resp = client.post(
        f"/api/plans/{PLAN}/exchange/batches/B-CLOSE/close",
        json={"force": False},
        headers=HEADERS,
    )
    assert resp.status_code == 409

    forced = client.post(
        f"/api/plans/{PLAN}/exchange/batches/B-CLOSE/close",
        json={"force": True, "reason": "term ended"},
        headers=HEADERS,
    ).json()
    assert forced["closed"] is True
    assert forced["batch"]["state"] == "closed"

    # 关闭后补交被拒
    resp = client.post(
        f"/api/plans/{PLAN}/exchange/batches/B-CLOSE/supplements",
        json={"events": [
            {"event_id": "E-LONG", "event_type": "checkin", "student_id": "S1",
             "references": 1, "resolution": "confirm",
             "payload": _checkin("E-LONG", "S1", hours=25)["payload"]},
        ]},
        headers=HEADERS,
    )
    assert resp.status_code == 409


def test_concurrent_close_only_one_wins(client):
    _create_plan(client)
    _receive(client, "B-RACE", [_checkin("E-1", "S1"), _checkin("E-2", "S1")])

    results: list[dict] = []
    barrier = threading.Barrier(2)

    def _close():
        session = TestSessionLocal()
        barrier.wait()
        try:
            results.append(
                exchange_service.close_batch(
                    session,
                    sender_id=SENDER,
                    plan_version=PLAN,
                    batch_id="B-RACE",
                    force=True,
                )
            )
        finally:
            session.close()

    threads = [threading.Thread(target=_close) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(r["closed"] for r in results) == [False, True]
    assert all(r["batch"]["state"] == "closed" for r in results)


def test_concurrent_close_vs_supplement(client, db):
    """关闭与补交并发：要么补交落地批次仍开放，要么补交被拒批次已关闭。"""

    _create_plan(client)
    _receive(client, "B-RACE2", [_checkin("E-LONG", "S1", hours=25)])
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    correction = {
        "event_id": "E-LONG",
        "event_type": "checkin",
        "student_id": "S1",
        "references": 1,
        "resolution": "confirm",
        "payload": _checkin("E-LONG", "S1", hours=25)["payload"],
    }

    def _supplement():
        barrier.wait()
        session = TestSessionLocal()
        try:
            exchange_service.submit_supplements(
                session,
                sender_id=SENDER,
                plan_version=PLAN,
                batch_id="B-RACE2",
                events=[correction],
            )
            outcomes.append("supplement-ok")
        except exchange_service.BatchClosedError:
            outcomes.append("supplement-rejected")
        finally:
            session.close()

    def _close():
        barrier.wait()
        session = TestSessionLocal()
        try:
            result = exchange_service.close_batch(
                session,
                sender_id=SENDER,
                plan_version=PLAN,
                batch_id="B-RACE2",
                force=True,
            )
            outcomes.append("closed" if result["closed"] else "close-lost")
        finally:
            session.close()

    t1 = threading.Thread(target=_supplement)
    t2 = threading.Thread(target=_close)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    db.expire_all()
    batch = exchange_service.require_batch(
        db, sender_id=SENDER, plan_version=PLAN, batch_id="B-RACE2"
    )
    receipt = exchange_service.get_receipts(
        db,
        sender_id=SENDER,
        plan_version=PLAN,
        batch_id="B-RACE2",
        cursor=0,
        limit=100,
        status=None,
    )

    if "supplement-ok" in outcomes:
        # 补交先拿锁并提交，关闭随后串行成功：修订已落地、原条目已解决。
        assert "closed" in outcomes
        assert batch.state == "closed"
        assert batch.revision == 2
        assert any(i["status"] == "resolved" for i in receipt["items"])
    else:
        # 关闭先提交：补交易整回滚，不留半成品回执行。
        assert "supplement-rejected" in outcomes
        assert "closed" in outcomes
        assert batch.state == "closed"
        assert batch.revision == 1
        assert [i["line_no"] for i in receipt["items"]] == [1]
        assert receipt["items"][0]["status"] == "pending_review"


# ---------------------------------------------------------------------------
# 重启恢复
# ---------------------------------------------------------------------------


def test_restart_resumes_from_cursor_without_duplicate_writes(client, db):
    _create_plan(client)
    n = 12
    events = [
        _checkin(f"E-{i:02d}", "S1", hours=1, start_day=(i % 27) + 1)
        for i in range(n)
    ]
    exchange_service.RESUME_FAULT_AFTER = 5
    with pytest.raises(RuntimeError):
        exchange_service.receive_batch(
            db,
            sender_id=SENDER,
            plan_version=PLAN,
            batch_id="B-CRASH",
            events=events,
        )

    # 模拟新进程：用全新会话重放同一批次。
    fresh = TestSessionLocal()
    try:
        batch = exchange_service.require_batch(
            fresh, sender_id=SENDER, plan_version=PLAN, batch_id="B-CRASH"
        )
        assert batch.cursor == 5
        result = exchange_service.receive_batch(
            fresh,
            sender_id=SENDER,
            plan_version=PLAN,
            batch_id="B-CRASH",
            events=events,
        )
    finally:
        fresh.close()

    assert result["replay"] is True
    assert result["resumed"] is True
    assert result["batch"]["cursor"] == n
    assert result["batch"]["accepted_count"] == n
    assert result["batch"]["duplicate_count"] == 0

    # 事件表恰好 n 行，无重复写入。
    count = (
        db.query(EventModel)
        .filter(EventModel.plan_version == PLAN)
        .count()
    )
    assert count == n

    # 再重放一次：回执保持已接受，事件表仍无重复。
    again = _receive(client, "B-CRASH", events)
    assert again["batch"]["accepted_count"] == n
    assert again["batch"]["duplicate_count"] == 0
    assert db.query(EventModel).filter(EventModel.plan_version == PLAN).count() == n

    # 跨批次重发相同事件：全部识别为重复。
    cross = _receive(client, "B-CRASH-COPY", events)
    assert cross["batch"]["accepted_count"] == 0
    assert cross["batch"]["duplicate_count"] == n
    assert db.query(EventModel).filter(EventModel.plan_version == PLAN).count() == n


# ---------------------------------------------------------------------------
# 对账
# ---------------------------------------------------------------------------


def test_reconcile_balanced_after_resolution(client):
    _create_plan(client)
    events = [
        {
            "event_id": "E-BAD",
            "event_type": "checkin",
            "student_id": "S1",
            "payload": {
                "check_in_at": "2024-03-03T10:00:00+08:00",
                "check_out_at": "2024-03-03T08:00:00+08:00",
            },
        },
        _checkin("E-OK", "S1"),
    ]
    _receive(client, "B-REC", events)

    before = client.get(
        f"/api/plans/{PLAN}/exchange/batches/B-REC/reconcile",
        headers=HEADERS,
    ).json()
    assert before["fingerprint_ok"] is True
    assert before["cursor_complete"] is True
    assert before["balanced"] is False
    assert before["events_expected"] == 1
    assert before["events_found"] == 1

    client.post(
        f"/api/plans/{PLAN}/exchange/batches/B-REC/supplements",
        json={"events": [
            {
                "event_id": "E-BAD",
                "event_type": "checkin",
                "student_id": "S1",
                "references": 1,
                "payload": {
                    "activity_id": "A1",
                    "activity_type": "regular",
                    "check_in_at": "2024-03-03T08:00:00+08:00",
                    "check_out_at": "2024-03-03T10:00:00+08:00",
                },
            },
        ]},
        headers=HEADERS,
    )
    after = client.get(
        f"/api/plans/{PLAN}/exchange/batches/B-REC/reconcile",
        headers=HEADERS,
    ).json()
    assert after["balanced"] is True
    assert after["events_expected"] == 2
    assert after["events_found"] == 2
    issues = {i["line_no"]: i["issue"] for i in after["items"] if i["issue"]}
    assert issues == {}
