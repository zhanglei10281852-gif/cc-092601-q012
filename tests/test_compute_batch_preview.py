from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection

from tests.test_compute_operations import TEMPLATE, create_template, submit_payload


def _submit(client, key, *, user="researcher-1", priority=50):
    response = client.post("/api/compute/tasks", json=submit_payload(key, user=user, priority=priority))
    assert response.status_code == 202, response.text
    return response.json()


def _claim(client, worker, expected_id):
    response = client.post(
        "/api/compute/tasks/claim",
        json={"worker_id": worker, "capabilities": ["solver-a"], "lease_seconds": 60},
    )
    assert response.status_code == 200, response.text
    assert response.json()["task"]["id"] == expected_id
    return response.json()["task"]


def _complete(client, task_id, worker):
    response = client.post(
        f"/api/compute/tasks/{task_id}/complete",
        json={"worker_id": worker, "result": {"v": 1}, "metrics": {}},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _preview(client, task_ids, operation, **extra):
    payload = {
        "operation": operation,
        "selector": {"task_ids": task_ids},
        "actor": "administrator",
        "reason": "错误参数发布后的批量处置",
        **extra,
    }
    response = client.post("/api/compute/batch-previews", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def _confirm(client, token, summary_digest, mode, **extra):
    return client.post(
        f"/api/compute/batch-previews/{token}/confirm",
        json={"confirmed_by": "administrator", "summary_digest": summary_digest, "execution_mode": mode, **extra},
    )


def test_preview_freezes_versions_actions_and_rejections(client):
    create_template(client)
    queued = _submit(client, "pv-0001")
    done = _submit(client, "pv-0002", priority=90)
    _claim(client, "w1", done["id"])
    _complete(client, done["id"], "w1")

    preview = _preview(client, [queued["id"], done["id"], 999999], "cancel")
    assert preview["counts"] == {"total": 3, "allowed": 1, "rejected": 2}
    by_task = {item["task_id"]: item for item in preview["items"]}
    allowed = by_task[queued["id"]]
    assert allowed["allowed"] is True
    assert allowed["version"] == queued["version"]
    assert allowed["status"] == "queued"
    assert "cancel" in allowed["allowed_actions"] and "priority" in allowed["allowed_actions"]
    assert by_task[done["id"]]["allowed"] is False
    assert "succeeded" in by_task[done["id"]]["reject_reason"]
    missing = by_task[999999]
    assert missing["allowed"] is False and missing["version"] is None
    assert "不存在" in missing["reject_reason"]
    # 选择条件与冻结后的任务清单一并指纹化
    assert len(preview["summary_digest"]) == 64 and len(preview["token"]) == 32


def test_confirm_all_or_nothing_succeeds_and_token_is_single_use(client):
    create_template(client)
    first = _submit(client, "cf-0001")
    second = _submit(client, "cf-0002")
    preview = _preview(client, [first["id"], second["id"]], "priority", priority=90)

    confirmed = _confirm(client, preview["token"], preview["summary_digest"], "all_or_nothing")
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body["replayed"] is False
    assert body["counts"] == {"succeeded": 2, "skipped": 0, "failed": 0}
    assert {item["task_id"] for item in body["succeeded"]} == {first["id"], second["id"]}
    assert all(item["priority"] == 90 for item in body["succeeded"])

    # 同一确认令牌重复使用：回放首次结果，不重复操作。
    replayed = _confirm(client, preview["token"], preview["summary_digest"], "all_or_nothing")
    assert replayed.status_code == 200
    replay_body = replayed.json()
    assert replay_body["replayed"] is True
    assert replay_body["counts"] == body["counts"]
    for task_id in (first["id"], second["id"]):
        details = client.get(f"/api/compute/task-details/{task_id}").json()
        assert len(details["interventions"]) == 1
        assert details["interventions"][0]["batch_key"] == preview["token"]


def test_confirm_rejects_tampered_summary(client):
    create_template(client)
    task = _submit(client, "cf-tamper")
    preview = _preview(client, [task["id"]], "cancel")
    response = _confirm(client, preview["token"], "0" * 64, "all_or_nothing")
    assert response.status_code == 409
    assert response.json()["error"]["context"]["code"] == "summary_mismatch"
    # 令牌仍然可用（校验失败不消耗令牌）
    ok = _confirm(client, preview["token"], preview["summary_digest"], "all_or_nothing")
    assert ok.status_code == 200


def test_all_or_nothing_aborts_on_drift_with_diffs(client):
    create_template(client)
    first = _submit(client, "drift-0001")
    second = _submit(client, "drift-0002")
    preview = _preview(client, [first["id"], second["id"]], "cancel")

    # 预演后、确认前，其中一条任务被其他流程取消，发生状态漂移。
    drifted = client.post(f"/api/compute/tasks/{first['id']}/cancel", json={"actor": "someone-else", "reason": "用户自行撤销"})
    assert drifted.status_code == 200

    response = _confirm(client, preview["token"], preview["summary_digest"], "all_or_nothing")
    assert response.status_code == 409
    context = response.json()["error"]["context"]
    assert context["code"] == "drift_detected"
    diff = context["diffs"][0]
    assert diff["task_id"] == first["id"]
    assert diff["preview_version"] == 1 and diff["current_version"] == 2
    assert diff["preview_status"] == "queued" and diff["current_status"] == "cancelled"

    # 整批回滚：未漂移的任务也没有被取消。
    untouched = client.get(f"/api/compute/task-details/{second['id']}").json()
    assert untouched["status"] == "queued" and untouched["interventions"] == []


def test_accept_partial_skips_drifted_and_rejected_items(client):
    create_template(client)
    queued = _submit(client, "partial-0001")
    running = _submit(client, "partial-0002", priority=90)
    succeeded = _submit(client, "partial-0003", priority=80)
    _claim(client, "w1", running["id"])
    _claim(client, "w2", succeeded["id"])
    _complete(client, succeeded["id"], "w2")
    preview = _preview(client, [queued["id"], running["id"], succeeded["id"]], "cancel")
    assert preview["counts"]["allowed"] == 2

    # queued 任务在确认前被他人改优先级，版本漂移但状态仍允许取消。
    client.post(
        f"/api/compute/tasks/{queued['id']}/priority",
        json={"actor": "scheduler", "reason": "动态调度", "priority": 5},
    )
    response = _confirm(client, preview["token"], preview["summary_digest"], "accept_partial")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["counts"]["succeeded"] == 1
    assert body["succeeded"][0]["task_id"] == running["id"]
    skip_codes = {item["task_id"]: item["reason_code"] for item in body["skipped"]}
    assert skip_codes[queued["id"]] == "version_changed"
    assert skip_codes[succeeded["id"]] == "rejected_at_preview"
    drift = next(item for item in body["diffs"] if item["task_id"] == queued["id"])
    assert drift["current_status"] == "queued" and drift["current_version"] == 2

    # 已确认的令牌不能再以别的模式重放执行。
    again = _confirm(client, preview["token"], preview["summary_digest"], "all_or_nothing")
    assert again.json()["replayed"] is True
    assert again.json()["execution_mode"] == "accept_partial"


def test_all_or_nothing_requires_no_preview_rejections(client):
    create_template(client)
    queued = _submit(client, "rej-0001")
    succeeded = _submit(client, "rej-0002", priority=90)
    _claim(client, "w1", succeeded["id"])
    _complete(client, succeeded["id"], "w1")
    preview = _preview(client, [queued["id"], succeeded["id"]], "cancel")
    response = _confirm(client, preview["token"], preview["summary_digest"], "all_or_nothing")
    assert response.status_code == 409
    assert response.json()["error"]["context"]["code"] == "preview_has_rejections"
    # 切换为接受逐条结果即可确认
    ok = _confirm(client, preview["token"], preview["summary_digest"], "accept_partial")
    assert ok.status_code == 200
    assert ok.json()["counts"] == {"succeeded": 1, "skipped": 1, "failed": 0}


def test_preview_by_filter_freezes_matching_task_set(client):
    create_template(client)
    mine = [_submit(client, f"flt-000{i}", user="project-owner") for i in range(3)]
    _submit(client, "flt-other", user="someone-else")
    response = client.post(
        "/api/compute/batch-previews",
        json={
            "operation": "priority",
            "selector": {"filter": {"requested_by": "project-owner"}},
            "actor": "administrator",
            "reason": "项目整体提级",
            "priority": 80,
        },
    )
    assert response.status_code == 201, response.text
    preview = response.json()
    assert preview["counts"]["total"] == 3
    assert {item["task_id"] for item in preview["items"]} == {task["id"] for task in mine}
    confirmed = _confirm(client, preview["token"], preview["summary_digest"], "all_or_nothing")
    assert confirmed.status_code == 200
    assert confirmed.json()["counts"]["succeeded"] == 3


def test_expired_token_reports_diffs_and_does_not_execute(client):
    create_template(client)
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    task = service.submit(submit_payload("ttl-0001"))
    preview = service.preview_batch({
        "operation": "cancel",
        "selector": {"task_ids": [task["id"]], "filter": None},
        "actor": "administrator",
        "reason": "令牌过期用例",
        "priority": None,
        "ttl_seconds": 60,
    })
    clock.advance(minutes=2)
    from app.core.errors import ConflictError

    with pytest.raises(ConflictError) as exc_info:
        service.confirm_batch_preview({
            "token": preview["token"],
            "confirmed_by": "administrator",
            "summary_digest": preview["summary_digest"],
            "execution_mode": "all_or_nothing",
            "include_rejected": False,
        })
    assert exc_info.value.context["code"] == "preview_expired"
    assert exc_info.value.context["expires_at"] == preview["expires_at"]
    details = service.get_task(task["id"])
    assert details["status"] == "queued" and details["interventions"] == []
    # 过期后需要重新预演：新预演产生新令牌且可正常确认
    new_preview = service.preview_batch({
        "operation": "cancel",
        "selector": {"task_ids": [task["id"]], "filter": None},
        "actor": "administrator",
        "reason": "重新预演",
        "priority": None,
        "ttl_seconds": 60,
    })
    result = service.confirm_batch_preview({
        "token": new_preview["token"],
        "confirmed_by": "administrator",
        "summary_digest": new_preview["summary_digest"],
        "execution_mode": "all_or_nothing",
        "include_rejected": False,
    })
    assert result["counts"]["succeeded"] == 1


def test_review_reconstructs_why_each_task_was_modified_or_skipped(client):
    create_template(client)
    queued = _submit(client, "review-0001")
    failed_task = _submit(client, "review-0002", priority=90)
    _claim(client, "w1", failed_task["id"])
    failed = client.post(
        f"/api/compute/tasks/{failed_task['id']}/fail",
        json={"worker_id": "w1", "error_code": "numeric", "message": "不收敛", "retryable": False},
    )
    assert failed.status_code == 200
    preview = _preview(client, [queued["id"], failed_task["id"]], "retry")
    assert preview["counts"] == {"total": 2, "allowed": 1, "rejected": 1}
    confirmed = _confirm(client, preview["token"], preview["summary_digest"], "accept_partial")
    assert confirmed.status_code == 200

    review = client.get(f"/api/compute/batch-previews/{preview['token']}/review")
    assert review.status_code == 200, review.text
    body = review.json()
    assert body["confirmation"]["confirmed_by"] == "administrator"
    assert body["confirmation"]["execution_mode"] == "accept_partial"
    assert body["confirmation"]["counts"] == {"succeeded": 1, "skipped": 1, "failed": 0}
    assert body["preview"]["actor"] == "administrator"
    assert body["selector"]["frozen_task_ids"] == [queued["id"], failed_task["id"]]

    by_task = {item["task_id"]: item for item in body["items"]}
    modified = by_task[failed_task["id"]]
    assert modified["execution"]["outcome"] == "succeeded"
    assert modified["intervention"]["action"] == "retry"
    assert modified["intervention"]["before"]["status"] == "failed"
    assert modified["intervention"]["after"]["status"] == "queued"
    assert modified["intervention"]["after"]["version"] == modified["intervention"]["before"]["version"] + 1

    skipped = by_task[queued["id"]]
    assert skipped["preview"]["allowed"] is False
    assert "queued" in skipped["preview"]["reject_reason"]
    assert skipped["execution"]["outcome"] == "skipped"
    assert skipped["execution"]["reason_code"] == "rejected_at_preview"
    assert skipped["intervention"] is None


def test_preview_get_endpoint_reports_live_drift(client):
    create_template(client)
    task = _submit(client, "live-0001")
    preview = _preview(client, [task["id"]], "cancel")
    client.post(f"/api/compute/tasks/{task['id']}/priority", json={"actor": "scheduler", "reason": "提级", "priority": 99})
    response = client.get(f"/api/compute/batch-previews/{preview['token']}")
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["changed"] is True
    assert item["preview_version"] == 1 and item["current_version"] == 2


def test_include_rejected_reevaluates_current_state(client):
    create_template(client)
    # 任务排队中，预演“重试”会被拒绝；随后工作者把它跑失败，状态变为可重试。
    task = _submit(client, "inc-0001", priority=90)
    preview = _preview(client, [task["id"]], "retry")
    assert preview["counts"]["rejected"] == 1
    _claim(client, "w1", task["id"])
    failed = client.post(
        f"/api/compute/tasks/{task['id']}/fail",
        json={"worker_id": "w1", "error_code": "numeric", "message": "不收敛", "retryable": False},
    )
    assert failed.status_code == 200

    # 默认仍按预演结论跳过；显式 include_rejected 后基于当前状态重新评估并执行。
    skipped = _confirm(client, preview["token"], preview["summary_digest"], "accept_partial")
    assert skipped.json()["counts"] == {"succeeded": 0, "skipped": 1, "failed": 0}
    # 第一次确认已经消耗令牌，需要重新预演才能带 include_rejected 再确认
    preview2 = _preview(client, [task["id"]], "retry")
    assert preview2["counts"]["allowed"] == 1
    executed = _confirm(
        client, preview2["token"], preview2["summary_digest"], "accept_partial", include_rejected=True
    )
    assert executed.status_code == 200
    assert executed.json()["counts"]["succeeded"] == 1
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["status"] == "queued"
    assert details["interventions"][-1]["batch_key"] == preview2["token"]


def test_all_or_nothing_include_rejected_still_blocked_aborts(client):
    create_template(client)
    queued = _submit(client, "aon-0001")
    succeeded = _submit(client, "aon-0002", priority=90)
    _claim(client, "w1", succeeded["id"])
    _complete(client, succeeded["id"], "w1")
    preview = _preview(client, [queued["id"], succeeded["id"]], "cancel")
    response = _confirm(
        client, preview["token"], preview["summary_digest"], "all_or_nothing", include_rejected=True
    )
    assert response.status_code == 409
    assert response.json()["error"]["context"]["code"] == "still_rejected"
    details = client.get(f"/api/compute/task-details/{queued['id']}").json()
    assert details["status"] == "queued" and details["interventions"] == []


def test_unknown_preview_token_returns_not_found(client):
    response = client.post(
        "/api/compute/batch-previews/0123456789abcdef/confirm",
        json={"confirmed_by": "administrator", "summary_digest": "0" * 64, "execution_mode": "all_or_nothing"},
    )
    assert response.status_code == 404
