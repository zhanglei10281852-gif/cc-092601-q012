from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import BatchDriftError, ConflictError, NotFoundError, PreviewDigestMismatchError, PreviewExpiredError, ValidationError
from app.database import get_connection, transaction

PREVIEW_TTL_SECONDS = 900
PREVIEW_TTL_MIN_SECONDS = 60
PREVIEW_TTL_MAX_SECONDS = 3600


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._mutation_for("cancel", None))

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "retry", batch_key, self._mutation_for("retry", priority))

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "priority", batch_key, self._mutation_for("priority", priority))

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def preview_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        """冻结选择条件，逐条记录任务当前版本、允许动作与拒绝原因，生成确认令牌。"""
        operation = str(payload["operation"])
        if operation not in {"cancel", "retry", "priority"}:
            raise ValidationError("不支持的批量操作")
        priority = payload.get("priority")
        if operation == "priority" and priority is None:
            raise ValidationError("批量调整优先级时必须提供 priority")
        task_ids = list(dict.fromkeys(int(task_id) for task_id in payload["task_ids"]))
        if not task_ids:
            raise ValidationError("批量预演至少需要一个任务")
        ttl = int(payload.get("ttl_seconds") or PREVIEW_TTL_SECONDS)
        ttl = max(PREVIEW_TTL_MIN_SECONDS, min(ttl, PREVIEW_TTL_MAX_SECONDS))
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires_at = to_storage(now_value + timedelta(seconds=ttl))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            items: list[dict[str, Any]] = []
            for task_id in task_ids:
                task = repository.task_by_id(task_id)
                allowed, code, message = self._evaluate_operation(operation, task)
                items.append({
                    "task_id": task_id,
                    "task_version": None if task is None else int(task["version"]),
                    "task_status": "" if task is None else str(task["status"]),
                    "allowed": allowed,
                    "reject_code": code,
                    "reject_message": message,
                })
            summary_digest = digest({
                "actor": payload["actor"], "operation": operation, "priority": priority, "reason": payload["reason"],
                "items": [{"task_id": item["task_id"], "task_version": item["task_version"], "allowed": item["allowed"], "reject_code": item["reject_code"]} for item in items],
            })
            allowed_count = sum(1 for item in items if item["allowed"])
            preview_id = repository.create_batch_preview(
                preview_token=f"bpv-{secrets.token_hex(16)}", operation=operation, actor=payload["actor"],
                reason=payload["reason"], priority=priority, selection={"task_ids": task_ids},
                summary_digest=summary_digest, total=len(items), allowed=allowed_count,
                rejected=len(items) - allowed_count, expires_at=expires_at, now=now,
            )
            for item in items:
                repository.add_batch_preview_item(preview_id=preview_id, **item)
            preview = repository.batch_preview_by_id(preview_id)
            return self._preview_payload(preview, repository.batch_preview_items(preview_id))

    def confirm_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        """校验预演摘要与任务版本后执行批量干预；令牌幂等，过期或漂移给出差异。"""
        mode = str(payload.get("mode") or "atomic")
        if mode not in {"atomic", "partial"}:
            raise ValidationError("确认模式必须是 atomic 或 partial")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            preview = repository.batch_preview_by_token(str(payload["preview_token"]))
            if preview is None:
                raise NotFoundError("批量预演不存在或确认令牌无效")
            existing = repository.batch_confirmation_by_token(preview["preview_token"])
            if existing is not None:
                replayed = json.loads(existing["result_json"])
                replayed["idempotent_replay"] = True
                return replayed
            if payload["summary_digest"] != preview["summary_digest"]:
                raise PreviewDigestMismatchError(
                    "预演摘要不匹配，请基于最新预演重新确认",
                    context={"expected": preview["summary_digest"], "provided": payload["summary_digest"]},
                )
            items = repository.batch_preview_items(preview["id"])
            drift = self._preview_drift(repository, items)
            if now > preview["expires_at"]:
                raise PreviewExpiredError(
                    "确认令牌已过期，请重新发起预演",
                    context={"expires_at": preview["expires_at"], "now": now, "drift": drift},
                )
            rejected_items = [item for item in items if not item["allowed"]]
            if mode == "atomic" and (drift or rejected_items):
                raise BatchDriftError(
                    "确认时发现任务漂移或不可操作条目，未执行任何修改",
                    context={
                        "drift": drift,
                        "rejected": [{"task_id": item["task_id"], "code": item["reject_code"], "message": item["reject_message"]} for item in rejected_items],
                    },
                )
            drift_by_task = {entry["task_id"]: entry for entry in drift}
            batch_key = digest({"preview_token": preview["preview_token"], "actor": payload["actor"], "mode": mode})
            mutation = self._mutation_for(preview["operation"], preview["priority"])
            applied: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []
            rejected: list[dict[str, Any]] = []
            for item in items:
                if not item["allowed"]:
                    rejected.append({"task_id": item["task_id"], "code": item["reject_code"], "message": item["reject_message"]})
                    repository.update_batch_preview_item_decision(item["id"], decision="rejected", reason=item["reject_message"], result_version=None, intervention_id=None)
                    continue
                entry = drift_by_task.get(item["task_id"])
                if entry is not None:
                    reason = self._drift_reason(entry)
                    skipped.append({"task_id": item["task_id"], "reason": entry["issue"], "detail": reason})
                    repository.update_batch_preview_item_decision(item["id"], decision="skipped", reason=reason, result_version=None, intervention_id=None)
                    continue
                task = repository.task_by_id(item["task_id"])
                before = dict(task)
                mutation(connection, task, now)
                after = dict(repository.task_by_id(item["task_id"]))
                intervention_id = repository.add_intervention(
                    task_id=item["task_id"], actor=payload["actor"], action=preview["operation"],
                    reason=preview["reason"], before=before, after=after, batch_key=batch_key, now=now,
                )
                applied.append({"task_id": item["task_id"], "version": after["version"], "intervention_id": intervention_id})
                repository.update_batch_preview_item_decision(item["id"], decision="applied", reason="", result_version=int(after["version"]), intervention_id=intervention_id)
            result = {
                "batch_key": batch_key,
                "preview_token": preview["preview_token"],
                "operation": preview["operation"],
                "mode": mode,
                "actor": payload["actor"],
                "status": "completed",
                "idempotent_replay": False,
                "applied": applied,
                "skipped": skipped,
                "rejected": rejected,
                "counts": {"applied": len(applied), "skipped": len(skipped), "rejected": len(rejected)},
            }
            repository.create_batch_confirmation(
                batch_key=batch_key, preview_id=preview["id"], preview_token=preview["preview_token"],
                mode=mode, actor=payload["actor"], applied=len(applied), skipped=len(skipped),
                rejected=len(rejected), result=result, now=now,
            )
            return result

    def get_batch_preview(self, preview_token: str) -> dict[str, Any]:
        preview = self.repository.batch_preview_by_token(preview_token)
        if preview is None:
            raise NotFoundError("批量预演不存在或确认令牌无效")
        payload = self._preview_payload(preview, self.repository.batch_preview_items(preview["id"]))
        confirmation = self.repository.batch_confirmation_by_token(preview_token)
        payload["confirmation"] = None if confirmation is None else {
            "batch_key": confirmation["batch_key"], "mode": confirmation["mode"],
            "actor": confirmation["actor"], "created_at": confirmation["created_at"],
        }
        return payload

    def get_batch(self, batch_key: str) -> dict[str, Any]:
        """按批次标识重建每条任务当时为何被修改或跳过。"""
        confirmation = self.repository.batch_confirmation_by_key(batch_key)
        if confirmation is None:
            raise NotFoundError("批次结果不存在")
        preview = self.repository.batch_preview_by_id(confirmation["preview_id"])
        items = self.repository.batch_preview_items(preview["id"])
        return {
            "batch": {
                "batch_key": confirmation["batch_key"],
                "mode": confirmation["mode"],
                "actor": confirmation["actor"],
                "status": confirmation["status"],
                "counts": {"applied": confirmation["applied_count"], "skipped": confirmation["skipped_count"], "rejected": confirmation["rejected_count"]},
                "result": json.loads(confirmation["result_json"]),
                "created_at": confirmation["created_at"],
            },
            "preview": self._preview_payload(preview, items),
            "items": [
                {
                    "task_id": item["task_id"],
                    "preview_version": item["task_version"],
                    "preview_status": item["task_status"] or None,
                    "allowed": bool(item["allowed"]),
                    "reject_code": item["reject_code"] or None,
                    "reject_message": item["reject_message"] or None,
                    "decision": item["decision"] or None,
                    "decision_reason": item["decision_reason"] or None,
                    "result_version": item["result_version"],
                    "intervention_id": item["intervention_id"],
                }
                for item in items
            ],
        }

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        allowed, _, message = ComputeOperationsService._evaluate_operation("cancel", task)
        if not allowed:
            raise ConflictError(message)
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

    @staticmethod
    def _evaluate_operation(operation: str, task: sqlite3.Row | None) -> tuple[bool, str, str]:
        """预演与确认共用的可执行性规则，返回 (是否允许, 拒绝代码, 拒绝原因)。"""
        if task is None:
            return False, "not_found", "计算任务不存在"
        status = task["status"]
        if operation == "cancel" and status not in {"queued", "running"}:
            return False, "state_not_allowed", "当前任务状态不允许取消"
        if operation == "retry" and status not in {"failed", "cancelled"}:
            return False, "state_not_allowed", "只有失败或已取消任务可以人工重试"
        if operation == "priority" and status not in {"queued", "running"}:
            return False, "state_not_allowed", "只有排队或运行中的任务可以调整优先级"
        return True, "", ""

    def _mutation_for(self, operation: str, priority: int | None) -> Callable[[sqlite3.Connection, sqlite3.Row, str], None]:
        if operation == "cancel":
            return self._cancel_mutation
        if operation == "retry":
            def mutate_retry(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
                allowed, _, message = self._evaluate_operation("retry", task)
                if not allowed:
                    raise ConflictError(message)
                chosen = task["priority"] if priority is None else priority
                connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
            return mutate_retry
        if operation == "priority":
            def mutate_priority(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
                allowed, _, message = self._evaluate_operation("priority", task)
                if not allowed:
                    raise ConflictError(message)
                connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
            return mutate_priority
        raise ValidationError("不支持的批量操作")

    @staticmethod
    def _preview_payload(preview: sqlite3.Row, items: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "preview_token": preview["preview_token"],
            "summary_digest": preview["summary_digest"],
            "operation": preview["operation"],
            "actor": preview["actor"],
            "reason": preview["reason"],
            "priority": preview["priority"],
            "selection": json.loads(preview["selection_json"]),
            "expires_at": preview["expires_at"],
            "created_at": preview["created_at"],
            "counts": {"total": preview["total_count"], "allowed": preview["allowed_count"], "rejected": preview["rejected_count"]},
            "items": [
                {
                    "task_id": item["task_id"],
                    "version": item["task_version"],
                    "status": item["task_status"] or None,
                    "allowed": bool(item["allowed"]),
                    "reject_code": item["reject_code"] or None,
                    "reject_message": item["reject_message"] or None,
                }
                for item in items
            ],
        }

    @staticmethod
    def _preview_drift(repository: ComputeRepository, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """重新读取每条任务并与预演冻结的版本对比，返回漂移差异。"""
        drift: list[dict[str, Any]] = []
        for item in items:
            task = repository.task_by_id(item["task_id"])
            expected = item["task_version"]
            if task is None:
                drift.append({"task_id": item["task_id"], "issue": "missing", "expected_version": expected, "current_version": None, "expected_status": item["task_status"] or None, "current_status": None})
            elif expected is None or int(task["version"]) != int(expected):
                drift.append({"task_id": item["task_id"], "issue": "version_changed", "expected_version": expected, "current_version": int(task["version"]), "expected_status": item["task_status"] or None, "current_status": task["status"]})
        return drift

    @staticmethod
    def _drift_reason(entry: dict[str, Any]) -> str:
        if entry["issue"] == "missing":
            return "确认时任务已不存在"
        return f"确认时任务版本漂移：预演版本 {entry['expected_version']}，当前版本 {entry['current_version']}"

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
