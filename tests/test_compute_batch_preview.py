from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import BatchDriftError, PreviewDigestMismatchError, PreviewExpiredError
from app.database import get_connection, init_db


TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "researcher-1", priority: int = 50) -> dict:
    return {
        "template_code": "solver-a",
        "project_code": "project-a",
        "requested_by": user,
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def submit(client, key: str, **kwargs) -> dict:
    response = client.post("/api/compute/tasks", json=submit_payload(key, **kwargs))
    assert response.status_code == 202, response.text
    return response.json()


def preview(client, task_ids, *, operation: str = "cancel", priority: int | None = None, ttl: int = 900) -> dict:
    payload = {"task_ids": task_ids, "operation": operation, "actor": "administrator", "reason": "错误参数发布回收", "ttl_seconds": ttl}
    if priority is not None:
        payload["priority"] = priority
    response = client.post("/api/compute/tasks/batch/preview", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def confirm(client, token: str, summary_digest: str, *, mode: str = "atomic", actor: str = "operator-2"):
    return client.post(
        "/api/compute/tasks/batch/confirm",
        json={"preview_token": token, "summary_digest": summary_digest, "mode": mode, "actor": actor},
    )


def test_preview_freezes_selection_and_lists_item_decisions(client):
    create_template(client)
    queued = submit(client, "preview-queued")
    cancelled = submit(client, "preview-cancelled")
    client.post(f"/api/compute/tasks/{cancelled['id']}/cancel", json={"actor": "administrator", "reason": "提前取消"})
    body = preview(client, [queued["id"], cancelled["id"], 99999])
    assert body["selection"] == {"task_ids": [queued["id"], cancelled["id"], 99999]}
    assert body["counts"] == {"total": 3, "allowed": 1, "rejected": 2}
    items = {item["task_id"]: item for item in body["items"]}
    assert items[queued["id"]]["allowed"] is True
    assert items[queued["id"]]["version"] == queued["version"]
    assert items[queued["id"]]["status"] == "queued"
    assert items[cancelled["id"]]["allowed"] is False
    assert items[cancelled["id"]]["reject_code"] == "state_not_allowed"
    assert items[99999]["allowed"] is False
    assert items[99999]["reject_code"] == "not_found"
    assert items[99999]["version"] is None
    # 预演之后新提交的任务不会进入已冻结的选择条件
    late = submit(client, "preview-late")
    fetched = client.get(f"/api/compute/batch-previews/{body['preview_token']}")
    assert fetched.status_code == 200
    assert late["id"] not in {item["task_id"] for item in fetched.json()["items"]}
    assert fetched.json()["confirmation"] is None


def test_atomic_confirm_applies_all_and_batch_query_reconstructs(client):
    create_template(client)
    first = submit(client, "atomic-one")
    second = submit(client, "atomic-two")
    body = preview(client, [first["id"], second["id"]], operation="priority", priority=99)
    confirmed = confirm(client, body["preview_token"], body["summary_digest"])
    assert confirmed.status_code == 200, confirmed.text
    result = confirmed.json()
    assert result["mode"] == "atomic"
    assert result["idempotent_replay"] is False
    assert result["counts"] == {"applied": 2, "skipped": 0, "rejected": 0}
    for task_id in (first["id"], second["id"]):
        details = client.get(f"/api/compute/task-details/{task_id}").json()
        assert details["priority"] == 99
        intervention = details["interventions"][-1]
        assert intervention["actor"] == "operator-2"
        assert intervention["action"] == "priority"
        assert intervention["reason"] == "错误参数发布回收"
        assert intervention["batch_key"] == result["batch_key"]
    batch = client.get(f"/api/compute/batches/{result['batch_key']}")
    assert batch.status_code == 200
    rebuilt = batch.json()
    assert rebuilt["batch"]["actor"] == "operator-2"
    assert rebuilt["preview"]["operation"] == "priority"
    assert rebuilt["preview"]["reason"] == "错误参数发布回收"
    for item in rebuilt["items"]:
        assert item["decision"] == "applied"
        assert item["result_version"] is not None
        assert item["intervention_id"] is not None
    # 预演查询也能关联到确认结果
    fetched = client.get(f"/api/compute/batch-previews/{body['preview_token']}").json()
    assert fetched["confirmation"]["batch_key"] == result["batch_key"]
    assert fetched["confirmation"]["actor"] == "operator-2"


def test_atomic_confirm_rejects_drift_without_any_mutation(client):
    create_template(client)
    stable = submit(client, "drift-stable")
    moved = submit(client, "drift-moved")
    body = preview(client, [stable["id"], moved["id"]])
    client.post(f"/api/compute/tasks/{moved['id']}/priority", json={"actor": "administrator", "reason": "临时调整", "priority": 80})
    response = confirm(client, body["preview_token"], body["summary_digest"])
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "batch_drift"
    assert error["context"]["drift"] == [
        {"task_id": moved["id"], "issue": "version_changed", "expected_version": 1, "current_version": 2, "expected_status": "queued", "current_status": "queued"}
    ]
    # 全有或全无：没有任何任务被批量修改，也没有留下批次干预记录
    stable_details = client.get(f"/api/compute/task-details/{stable['id']}").json()
    moved_details = client.get(f"/api/compute/task-details/{moved['id']}").json()
    assert stable_details["version"] == stable["version"]
    assert stable_details["interventions"] == []
    assert moved_details["version"] == moved["version"] + 1
    assert [item["action"] for item in moved_details["interventions"]] == ["priority"]
    assert client.get(f"/api/compute/batch-previews/{body['preview_token']}").json()["confirmation"] is None


def test_partial_confirm_skips_drifted_and_keeps_rejected(client):
    create_template(client)
    apply_me = submit(client, "partial-apply")
    drifted = submit(client, "partial-drifted")
    refused = submit(client, "partial-refused")
    client.post(f"/api/compute/tasks/{refused['id']}/cancel", json={"actor": "administrator", "reason": "提前取消"})
    body = preview(client, [apply_me["id"], drifted["id"], refused["id"]])
    assert body["counts"] == {"total": 3, "allowed": 2, "rejected": 1}
    client.post(f"/api/compute/tasks/{drifted['id']}/priority", json={"actor": "administrator", "reason": "临时调整", "priority": 70})
    confirmed = confirm(client, body["preview_token"], body["summary_digest"], mode="partial")
    assert confirmed.status_code == 200, confirmed.text
    result = confirmed.json()
    assert result["counts"] == {"applied": 1, "skipped": 1, "rejected": 1}
    assert result["applied"] == [{"task_id": apply_me["id"], "version": apply_me["version"] + 1, "intervention_id": result["applied"][0]["intervention_id"]}]
    assert result["skipped"][0]["task_id"] == drifted["id"]
    assert result["skipped"][0]["reason"] == "version_changed"
    assert result["rejected"] == [{"task_id": refused["id"], "code": "state_not_allowed", "message": "当前任务状态不允许取消"}]
    assert client.get(f"/api/compute/task-details/{apply_me['id']}").json()["status"] == "cancelled"
    assert client.get(f"/api/compute/task-details/{drifted['id']}").json()["status"] == "queued"
    # 批次查询可以重建每条任务被修改或跳过的原因
    rebuilt = client.get(f"/api/compute/batches/{result['batch_key']}").json()
    decisions = {item["task_id"]: item for item in rebuilt["items"]}
    assert decisions[apply_me["id"]]["decision"] == "applied"
    assert decisions[drifted["id"]]["decision"] == "skipped"
    assert "版本漂移" in decisions[drifted["id"]]["decision_reason"]
    assert decisions[refused["id"]]["decision"] == "rejected"
    assert decisions[refused["id"]]["decision_reason"] == "当前任务状态不允许取消"


def test_confirm_token_replay_never_repeats_operations(client):
    create_template(client)
    task = submit(client, "replay-task")
    body = preview(client, [task["id"]])
    first = confirm(client, body["preview_token"], body["summary_digest"])
    assert first.status_code == 200
    version_after_first = client.get(f"/api/compute/task-details/{task['id']}").json()["version"]
    second = confirm(client, body["preview_token"], body["summary_digest"])
    assert second.status_code == 200
    replay = second.json()
    assert replay["idempotent_replay"] is True
    assert replay["batch_key"] == first.json()["batch_key"]
    assert replay["counts"] == {"applied": 1, "skipped": 0, "rejected": 0}
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["version"] == version_after_first
    assert len(details["interventions"]) == 1


def test_confirm_rejects_wrong_digest_and_unknown_token(client):
    create_template(client)
    task = submit(client, "digest-task")
    body = preview(client, [task["id"]])
    mismatch = confirm(client, body["preview_token"], "0" * 64)
    assert mismatch.status_code == 409
    assert mismatch.json()["error"]["code"] == "preview_digest_mismatch"
    assert mismatch.json()["error"]["context"]["expected"] == body["summary_digest"]
    missing = confirm(client, "bpv-does-not-exist", body["summary_digest"])
    assert missing.status_code == 404
    # 摘要错误不会消耗令牌，修正后仍可确认
    confirmed = confirm(client, body["preview_token"], body["summary_digest"])
    assert confirmed.status_code == 200


def test_expired_token_reports_expiry_and_drift(client):
    create_template(client)
    task = submit(client, "expired-task")
    body = preview(client, [task["id"]], ttl=60)
    client.post(f"/api/compute/tasks/{task['id']}/priority", json={"actor": "administrator", "reason": "临时调整", "priority": 60})
    connection = get_connection()
    connection.execute("UPDATE compute_batch_previews SET expires_at=? WHERE preview_token=?", ("2000-01-01T00:00:00+00:00", body["preview_token"]))
    response = confirm(client, body["preview_token"], body["summary_digest"])
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "preview_expired"
    assert error["context"]["expires_at"] == "2000-01-01T00:00:00+00:00"
    assert error["context"]["drift"][0]["task_id"] == task["id"]
    assert error["context"]["drift"][0]["issue"] == "version_changed"
    assert client.get(f"/api/compute/task-details/{task['id']}").json()["status"] == "queued"


def test_batch_retry_and_priority_operations(client):
    create_template(client)
    cancelled = submit(client, "retry-cancelled")
    client.post(f"/api/compute/tasks/{cancelled['id']}/cancel", json={"actor": "administrator", "reason": "参数错误"})
    retried_preview = preview(client, [cancelled["id"]], operation="retry", priority=88)
    retried = confirm(client, retried_preview["preview_token"], retried_preview["summary_digest"])
    assert retried.status_code == 200
    details = client.get(f"/api/compute/task-details/{cancelled['id']}").json()
    assert details["status"] == "queued"
    assert details["priority"] == 88
    assert details["interventions"][-1]["action"] == "retry"
    running_candidate = submit(client, "priority-queued")
    priority_preview = preview(client, [running_candidate["id"]], operation="priority", priority=5)
    lowered = confirm(client, priority_preview["preview_token"], priority_preview["summary_digest"], mode="partial")
    assert lowered.status_code == 200
    assert client.get(f"/api/compute/task-details/{running_candidate['id']}").json()["priority"] == 5


def test_service_level_ttl_and_atomic_all_or_nothing(client):
    init_db()
    clock = FrozenClock(datetime(2026, 9, 27, 8, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    first = service.submit(submit_payload("svc-first"))
    second = service.submit(submit_payload("svc-second"))
    body = service.preview_batch({"task_ids": [first["id"], second["id"]], "operation": "cancel", "actor": "administrator", "reason": "参数回收", "priority": None, "ttl_seconds": 120})
    service.set_priority(first["id"], "administrator", "确认前漂移", 65)
    clock.advance(seconds=30)
    with pytest.raises(BatchDriftError) as caught:
        service.confirm_batch({"preview_token": body["preview_token"], "summary_digest": body["summary_digest"], "mode": "atomic", "actor": "operator"})
    assert caught.value.context["drift"][0]["task_id"] == first["id"]
    assert service.get_task(second["id"])["version"] == second["version"]
    clock.advance(seconds=120)
    with pytest.raises(PreviewExpiredError) as expired:
        service.confirm_batch({"preview_token": body["preview_token"], "summary_digest": body["summary_digest"], "mode": "partial", "actor": "operator"})
    assert expired.value.context["drift"][0]["issue"] == "version_changed"
    fresh = service.preview_batch({"task_ids": [first["id"], second["id"]], "operation": "cancel", "actor": "administrator", "reason": "参数回收", "priority": None, "ttl_seconds": 120})
    with pytest.raises(PreviewDigestMismatchError):
        service.confirm_batch({"preview_token": fresh["preview_token"], "summary_digest": "f" * 64, "mode": "atomic", "actor": "operator"})
    done = service.confirm_batch({"preview_token": fresh["preview_token"], "summary_digest": fresh["summary_digest"], "mode": "atomic", "actor": "operator"})
    assert done["counts"] == {"applied": 2, "skipped": 0, "rejected": 0}
    assert service.get_task(first["id"])["status"] == "cancelled"
