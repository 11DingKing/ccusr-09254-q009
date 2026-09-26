"""外部事件交换批次与逐条回执的测试。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from app.exchange import service as exchange_service
from app.models import Base
from app.models import Event as EventModel
from app.repository import upsert_plan
from tests.conftest import SHANGHAI_PLAN

PLAN = SHANGHAI_PLAN["plan_version"]
SENDER_A = {"X-Sender-Id": "college-a"}
SENDER_B = {"X-Sender-Id": "college-b"}


def _checkin(
    item_id,
    student="S1",
    start="2024-03-15T08:00:00+08:00",
    end="2024-03-15T10:00:00+08:00",
    **extra,
):
    item = {
        "client_item_id": item_id,
        "event_type": "checkin",
        "student_id": student,
        "payload": {
            "activity_id": "A1",
            "activity_type": "regular",
            "check_in_at": start,
            "check_out_at": end,
        },
    }
    item.update(extra)
    return item


def _create_plan(client):
    resp = client.post("/api/plans", json=SHANGHAI_PLAN)
    assert resp.status_code == 201, resp.text


def _receive(client, batch_id, items, headers=SENDER_A, plan_version=PLAN, schema_version="1.0"):
    return client.post(
        "/api/exchange/batches",
        json={
            "batch_id": batch_id,
            "plan_version": plan_version,
            "schema_version": schema_version,
            "items": items,
        },
        headers=headers,
    )


def _supplement(client, batch_id, items, headers=SENDER_A):
    return client.post(
        f"/api/exchange/batches/{batch_id}/supplements",
        json={"items": items},
        headers=headers,
    )


def _status_map(body):
    return {r["client_item_id"]: r["status"] for r in body["receipts"]}


def _event_count(db, plan_version=PLAN):
    return db.execute(
        select(func.count(EventModel.id)).where(EventModel.plan_version == plan_version)
    ).scalar_one()


def test_receive_batch_distinguishes_receipt_states(client):
    _create_plan(client)
    _receive(
        client,
        "B-PRE",
        [
            _checkin("E-00"),
            _checkin("E-09", start="2024-03-16T08:00:00+08:00", end="2024-03-16T10:00:00+08:00"),
        ],
    )

    items = [
        _checkin("E-01"),  # 新事件 -> accepted
        _checkin("E-00"),  # 与事件流一致 -> duplicate
        _checkin("E-09", start="2024-03-16T08:00:00+08:00", end="2024-03-16T12:00:00+08:00"),  # 与已接受事件冲突
        {"client_item_id": "E-03", "event_type": "teleport", "student_id": "S1", "payload": {}},  # 未知类型 -> 待审核
        {"client_item_id": "E-04", "event_type": "mentor_confirm", "student_id": "S1", "payload": {"checkin_event_id": "E-99"}},  # 引用缺失 -> 待审核
        _checkin("E-05", start="2024-03-15T10:00:00+08:00", end="2024-03-15T08:00:00+08:00"),  # 非法载荷 -> conflict
    ]
    resp = _receive(client, "B-STATES", items)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert _status_map(body) == {
        "E-01": "accepted",
        "E-00": "duplicate",
        "E-09": "conflict",
        "E-03": "pending_review",
        "E-04": "pending_review",
        "E-05": "conflict",
    }
    assert body["accepted_count"] == 1
    assert body["duplicate_count"] == 1
    assert body["conflict_count"] == 2
    assert body["pending_count"] == 2
    assert body["replay_cursor"] == 6
    assert body["item_count"] == 6
    assert len(body["content_fingerprint"]) == 64
    assert body["status"] == "open"
    assert body["version"] == 1


def test_receiving_same_batch_twice_replays_stored_receipts(client, db):
    _create_plan(client)
    items = [
        _checkin("E-20"),
        _checkin("E-21", start="2024-03-15T12:00:00+08:00", end="2024-03-15T14:00:00+08:00"),
    ]
    first = _receive(client, "B-TWICE", items)
    assert first.status_code == 201
    second = _receive(client, "B-TWICE", items)
    assert second.status_code == 200
    assert second.json()["content_fingerprint"] == first.json()["content_fingerprint"]
    assert second.json()["receipts"] == first.json()["receipts"]

    # 相同批次号但内容不同 -> 拒绝
    changed = _receive(client, "B-TWICE", [_checkin("E-22")])
    assert changed.status_code == 409

    assert _event_count(db) == 2


def test_large_batch_retry_never_double_writes(client, db):
    _create_plan(client)
    items = [_checkin(f"E-{i:04d}") for i in range(300)]
    resp = _receive(client, "B-LARGE", items)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["accepted_count"] == 300
    assert body["replay_cursor"] == 300

    # 网络重试：相同批次号与相同内容 -> 幂等重放已存回执
    replay = _receive(client, "B-LARGE", items)
    assert replay.status_code == 200
    assert replay.json()["content_fingerprint"] == body["content_fingerprint"]
    assert len(replay.json()["receipts"]) == 300

    # 换批次号重发同样事件 -> 全部判重，不再写入
    again = _receive(client, "B-LARGE-RETRY", items)
    assert again.status_code == 201
    assert again.json()["duplicate_count"] == 300
    assert again.json()["accepted_count"] == 0

    assert _event_count(db) == 300

    report = client.get("/api/exchange/batches/B-LARGE/reconciliation", headers=SENDER_A).json()
    assert report["balanced"]
    assert report["event_stream_accepted"] == 300


def test_in_batch_conflict_and_supplement_retry(client):
    _create_plan(client)
    items = [
        _checkin("E-10"),
        _checkin("E-10", end="2024-03-15T12:00:00+08:00"),  # 批内同号不同内容 -> conflict
        _checkin("E-11", start="2024-03-15T10:00:00+08:00", end="2024-03-15T08:00:00+08:00"),  # 非法 -> conflict
    ]
    resp = _receive(client, "B-INBATCH", items)
    assert resp.status_code == 201
    body = resp.json()
    receipts = body["receipts"]
    assert receipts[0]["status"] == "accepted"
    assert receipts[1]["status"] == "conflict"
    assert "within this batch" in receipts[1]["detail"]
    assert receipts[2]["status"] == "conflict"
    assert body["accepted_count"] == 1
    assert body["conflict_count"] == 2

    # 修正 E-11 后引用原条目补交
    fixed = _checkin("E-11", start="2024-03-15T12:00:00+08:00", end="2024-03-15T14:00:00+08:00")
    fixed["retry_of"] = "E-11"
    resp = _supplement(client, "B-INBATCH", [fixed])
    assert resp.status_code == 201, resp.text
    sup = resp.json()
    assert sup["receipts"][0]["status"] == "accepted"
    assert sup["receipts"][0]["attempt"] == 2
    assert sup["receipts"][0]["retry_of"] == "E-11"
    assert sup["accepted_count"] == 2
    assert sup["conflict_count"] == 1
    assert sup["superseded_count"] == 1
    assert sup["replay_cursor"] == 4

    # 原条目转为 superseded，新条目 accepted，审计轨迹完整
    page = client.get("/api/exchange/batches/B-INBATCH/receipts", headers=SENDER_A).json()
    by_seq = {r["item_seq"]: r for r in page["receipts"]}
    assert by_seq[3]["status"] == "superseded"
    assert by_seq[4]["status"] == "accepted"

    # 修正后的事件进入事件流并参与学时重放
    progress = client.get(f"/api/plans/{PLAN}/students/S1/progress").json()
    assert progress["total_seconds"] == 2 * 7200


def test_supplement_can_be_retried_until_accepted(client):
    _create_plan(client)
    bad = _checkin("E-45", start="2024-03-15T10:00:00+08:00", end="2024-03-15T08:00:00+08:00")
    _receive(client, "B-MULTI", [bad])

    still_bad = _checkin("E-45", start="2024-03-15T11:00:00+08:00", end="2024-03-15T10:00:00+08:00")
    still_bad["retry_of"] = "E-45"
    first = _supplement(client, "B-MULTI", [still_bad]).json()["receipts"][0]
    assert first["status"] == "conflict"
    assert first["attempt"] == 2

    fixed = _checkin("E-45")
    fixed["retry_of"] = "E-45"
    second = _supplement(client, "B-MULTI", [fixed]).json()["receipts"][0]
    assert second["status"] == "accepted"
    assert second["attempt"] == 3

    page = client.get("/api/exchange/batches/B-MULTI/receipts", headers=SENDER_A).json()
    assert [r["status"] for r in page["receipts"]] == ["superseded", "superseded", "accepted"]


def test_accepted_items_cannot_be_rewritten(client, db):
    _create_plan(client)
    resp = _receive(client, "B-IMMUTABLE", [_checkin("E-20")])
    assert resp.json()["accepted_count"] == 1

    # 已接受条目不是合法的补交目标
    forged = _checkin("E-20", end="2024-03-15T13:00:00+08:00")
    forged["retry_of"] = "E-20"
    resp = _supplement(client, "B-IMMUTABLE", [forged])
    assert resp.status_code == 201
    receipt = resp.json()["receipts"][0]
    assert receipt["status"] == "conflict"
    assert "cannot be corrected" in receipt["detail"]

    # 换批次重发同号不同内容同样只会冲突
    forged.pop("retry_of")
    resp = _receive(client, "B-IMMUTABLE-2", [forged])
    assert _status_map(resp.json())["E-20"] == "conflict"

    # 事件流保持原始内容
    assert _event_count(db) == 1
    progress = client.get(f"/api/plans/{PLAN}/students/S1/progress").json()
    assert progress["total_seconds"] == 7200


def test_pending_review_item_becomes_accepted_after_supplement(client):
    _create_plan(client)
    confirm = {
        "client_item_id": "E-71",
        "event_type": "mentor_confirm",
        "student_id": "S1",
        "payload": {"checkin_event_id": "E-70"},
    }
    resp = _receive(client, "B-PENDING", [confirm])
    assert _status_map(resp.json())["E-71"] == "pending_review"

    # 后续批次带来被引用的签到，待审核条目即可补交转正
    _receive(client, "B-PENDING-2", [_checkin("E-70")])
    retry = dict(confirm)
    retry["retry_of"] = "E-71"
    resp = _supplement(client, "B-PENDING", [retry])
    receipt = resp.json()["receipts"][0]
    assert receipt["status"] == "accepted"
    assert resp.json()["pending_count"] == 0
    assert resp.json()["superseded_count"] == 1


def test_supplement_validates_retry_references(client):
    _create_plan(client)
    _receive(client, "B-SUP", [_checkin("E-41")])

    # 缺少 retry_of
    resp = _supplement(client, "B-SUP", [_checkin("E-42")])
    assert resp.status_code == 201
    receipt = resp.json()["receipts"][0]
    assert receipt["status"] == "conflict"
    assert "retry_of" in receipt["detail"]

    # retry_of 指向不存在的条目
    item = _checkin("E-43")
    item["retry_of"] = "E-99"
    resp = _supplement(client, "B-SUP", [item])
    receipt = resp.json()["receipts"][0]
    assert receipt["status"] == "conflict"
    assert "not found" in receipt["detail"]


def test_close_batch_is_versioned_and_seals_the_batch(client):
    _create_plan(client)
    _receive(client, "B-CLOSE", [_checkin("E-30")])

    resp = client.post(
        "/api/exchange/batches/B-CLOSE/close", json={"expected_version": 5}, headers=SENDER_A
    )
    assert resp.status_code == 409

    resp = client.post(
        "/api/exchange/batches/B-CLOSE/close", json={"expected_version": 1}, headers=SENDER_A
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "sealed"
    assert body["version"] == 2
    assert body["closed_at"] is not None

    # 当前版本重复关闭 -> 幂等返回相同状态
    resp = client.post(
        "/api/exchange/batches/B-CLOSE/close", json={"expected_version": 2}, headers=SENDER_A
    )
    assert resp.status_code == 200
    assert resp.json()["version"] == 2

    # 旧版本 -> 冲突
    resp = client.post(
        "/api/exchange/batches/B-CLOSE/close", json={"expected_version": 1}, headers=SENDER_A
    )
    assert resp.status_code == 409

    # 关闭后禁止补交
    item = _checkin("E-31")
    item["retry_of"] = "E-30"
    resp = _supplement(client, "B-CLOSE", [item])
    assert resp.status_code == 409


def test_receipts_follow_the_replay_cursor(client):
    _create_plan(client)
    items = [
        _checkin(f"E-7{i}", start=f"2024-03-1{i}T08:00:00+08:00", end=f"2024-03-1{i}T10:00:00+08:00")
        for i in range(5)
    ]
    _receive(client, "B-CURSOR", items)

    seen = []
    cursor = 0
    while True:
        page = client.get(
            f"/api/exchange/batches/B-CURSOR/receipts?after_seq={cursor}&limit=2",
            headers=SENDER_A,
        ).json()
        seen.extend(r["item_seq"] for r in page["receipts"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
    assert seen == [1, 2, 3, 4, 5]
    assert cursor == 5

    # 补交后游标继续前进，断点续拉不丢不重
    item = _checkin("E-70")
    item["retry_of"] = "E-70"
    _supplement(client, "B-CURSOR", [item])
    page = client.get(
        "/api/exchange/batches/B-CURSOR/receipts?after_seq=5&limit=2", headers=SENDER_A
    ).json()
    assert [r["item_seq"] for r in page["receipts"]] == [6]
    assert page["replay_cursor"] == 6
    assert page["has_more"] is False


def test_reconciliation_balances_receipts_against_the_event_stream(client):
    _create_plan(client)
    _receive(client, "B-RECON-PRE", [_checkin("E-80")])

    items = [
        _checkin("E-81", start="2024-03-15T12:00:00+08:00", end="2024-03-15T14:00:00+08:00"),
        _checkin("E-80"),  # duplicate
        _checkin("E-80", end="2024-03-15T13:00:00+08:00"),  # 与批内条目冲突
        _checkin("E-82", start="2024-03-15T10:00:00+08:00", end="2024-03-15T08:00:00+08:00"),  # 非法
    ]
    resp = _receive(client, "B-RECON", items)
    fingerprint = resp.json()["content_fingerprint"]

    report = client.get(
        f"/api/exchange/batches/B-RECON/reconciliation?fingerprint={fingerprint}",
        headers=SENDER_A,
    ).json()
    assert report["balanced"]
    assert report["counts_consistent"]
    assert report["event_stream_accepted"] == 1
    assert report["fingerprint_match"]
    assert report["accepted_count"] == 1
    assert report["duplicate_count"] == 1
    assert report["conflict_count"] == 2

    tampered = client.get(
        "/api/exchange/batches/B-RECON/reconciliation?fingerprint=" + "f" * 64,
        headers=SENDER_A,
    ).json()
    assert tampered["fingerprint_match"] is False


def test_sender_can_only_see_own_batches(client):
    _create_plan(client)
    resp = _receive(client, "B-PRIVATE", [_checkin("E-60")], headers=SENDER_A)
    assert resp.status_code == 201

    # 其他发送方查询、补交、关闭、对账均不可见
    assert client.get("/api/exchange/batches/B-PRIVATE/receipts", headers=SENDER_B).status_code == 404
    assert client.get("/api/exchange/batches/B-PRIVATE/reconciliation", headers=SENDER_B).status_code == 404
    item = _checkin("E-60")
    item["retry_of"] = "E-60"
    assert _supplement(client, "B-PRIVATE", [item], headers=SENDER_B).status_code == 404
    assert (
        client.post(
            "/api/exchange/batches/B-PRIVATE/close",
            json={"expected_version": 1},
            headers=SENDER_B,
        ).status_code
        == 404
    )

    # 相同批次号对其他发送方是独立命名空间
    resp = _receive(client, "B-PRIVATE", [_checkin("E-61")], headers=SENDER_B)
    assert resp.status_code == 201
    assert resp.json()["sender_id"] == "college-b"

    mine = client.get("/api/exchange/batches/B-PRIVATE/receipts", headers=SENDER_A).json()
    assert mine["accepted_count"] == 1
    assert mine["receipts"][0]["client_item_id"] == "E-60"


def test_receive_requires_supported_schema_version(client):
    _create_plan(client)
    resp = _receive(client, "B-SCHEMA", [_checkin("E-90")], schema_version="9.9")
    assert resp.status_code == 422


def test_receive_requires_existing_plan(client):
    resp = _receive(client, "B-NOPLAN", [_checkin("E-91")], plan_version="NOPE")
    assert resp.status_code == 404


def test_exchange_endpoints_require_sender_header(client):
    _create_plan(client)
    resp = client.post(
        "/api/exchange/batches",
        json={"batch_id": "B-H", "plan_version": PLAN, "schema_version": "1.0", "items": []},
    )
    assert resp.status_code == 422


def _make_session_factory(tmp_path, name="exchange.db"):
    engine = create_engine(
        f"sqlite:///{tmp_path / name}",
        connect_args={"check_same_thread": False, "timeout": 30},
        future=True,
    )
    Base.metadata.create_all(engine)
    return engine, sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def test_concurrent_close_allows_a_single_winner(tmp_path):
    engine, Session = _make_session_factory(tmp_path)
    with Session() as session:
        upsert_plan(session, plan_version="P-C", iana_timezone="Asia/Shanghai", required_seconds=0)
        exchange_service.receive_batch(
            session,
            sender_id="college-a",
            batch_id="B-RACE",
            plan_version="P-C",
            schema_version="1.0",
            items=[_checkin("E-40")],
        )

    barrier = Barrier(2)

    def _close():
        barrier.wait()
        with Session() as session:
            try:
                exchange_service.close_batch(
                    session, sender_id="college-a", batch_id="B-RACE", expected_version=1
                )
                return "sealed"
            except exchange_service.VersionConflictError:
                return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(_close) for _ in range(2)]
        outcomes = sorted(f.result() for f in futures)
    assert outcomes == ["conflict", "sealed"]

    with Session() as session:
        header = exchange_service.get_receipts(session, sender_id="college-a", batch_id="B-RACE")
        assert header["status"] == "sealed"
        assert header["version"] == 2
    engine.dispose()


def test_restart_recovers_batch_state(tmp_path):
    url = f"sqlite:///{tmp_path / 'restart.db'}"
    engine = create_engine(
        url, connect_args={"check_same_thread": False, "timeout": 30}, future=True
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    original_items = [
        _checkin("E-50"),
        _checkin("E-51", start="2024-03-15T10:00:00+08:00", end="2024-03-15T08:00:00+08:00"),
    ]
    with Session() as session:
        upsert_plan(session, plan_version="P-R", iana_timezone="Asia/Shanghai", required_seconds=3600)
        payload, created = exchange_service.receive_batch(
            session,
            sender_id="college-a",
            batch_id="B-RESTART",
            plan_version="P-R",
            schema_version="1.0",
            items=original_items,
        )
        assert created
        intake_fingerprint = payload["intake_fingerprint"]

        fixed = _checkin("E-51", start="2024-03-15T12:00:00+08:00", end="2024-03-15T14:00:00+08:00")
        fixed["retry_of"] = "E-51"
        supplement = exchange_service.supplement_batch(
            session, sender_id="college-a", batch_id="B-RESTART", items=[fixed]
        )
        assert supplement["receipts"][0]["status"] == "accepted"
        content_fingerprint = supplement["content_fingerprint"]

        header = exchange_service.close_batch(
            session, sender_id="college-a", batch_id="B-RESTART", expected_version=1
        )
        assert header["status"] == "sealed"
    engine.dispose()

    # 模拟服务重启：重新连接同一数据库文件，内存状态全部丢失
    engine2 = create_engine(
        url, connect_args={"check_same_thread": False, "timeout": 30}, future=True
    )
    Base.metadata.create_all(engine2)
    Session2 = sessionmaker(bind=engine2, autoflush=False, autocommit=False, future=True)
    with Session2() as session:
        page = exchange_service.get_receipts(session, sender_id="college-a", batch_id="B-RESTART")
        assert page["status"] == "sealed"
        assert page["version"] == 2
        assert page["intake_fingerprint"] == intake_fingerprint
        assert page["content_fingerprint"] == content_fingerprint
        assert page["replay_cursor"] == 3
        assert page["accepted_count"] == 2
        assert page["superseded_count"] == 1

        # 重放原始批次仍然幂等，不会重复写入
        replayed, created = exchange_service.receive_batch(
            session,
            sender_id="college-a",
            batch_id="B-RESTART",
            plan_version="P-R",
            schema_version="1.0",
            items=original_items,
        )
        assert not created
        assert replayed["intake_fingerprint"] == intake_fingerprint

        # 对账平衡：两条已接受条目都在事件流中
        report = exchange_service.reconcile_batch(
            session,
            sender_id="college-a",
            batch_id="B-RESTART",
            expected_fingerprint=content_fingerprint,
        )
        assert report["balanced"]
        assert report["counts_consistent"]
        assert report["event_stream_accepted"] == 2
        assert report["fingerprint_match"]
    engine2.dispose()
