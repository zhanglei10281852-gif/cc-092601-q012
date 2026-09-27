from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


BATCH_ALLOWED_STATUSES: dict[str, set[str]] = {
    "cancel": {"queued", "running"},
    "retry": {"failed", "cancelled"},
    "priority": {"queued", "running"},
}


def allowed_actions_for_status(status: str) -> list[str]:
    return [action for action, states in BATCH_ALLOWED_STATUSES.items() if status in states]


def _reject_reason(operation: str, status: str) -> str:
    if operation == "cancel":
        return f"当前状态 {status} 不允许取消（仅 queued/running 可取消）"
    if operation == "retry":
        return f"当前状态 {status} 不允许人工重试（仅 failed/cancelled 可重试）"
    return f"当前状态 {status} 不允许调整优先级（仅 queued/running 可调）"


def _snapshot(value: dict[str, Any]) -> dict[str, Any]:
    """干预前后快照只保留解释变更所需的任务字段，避免泄露参数全文。"""
    keys = ("id", "status", "priority", "attempt_count", "version", "lease_owner",
            "available_at", "finished_at", "last_error_code", "updated_at")
    return {key: value.get(key) for key in keys if key in value}


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
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

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

    # ------------------------------------------------------------------
    # 两阶段批量操作：预演（preview）→ 确认（confirm）
    # ------------------------------------------------------------------

    def preview_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        """冻结选择条件，逐条给出当前版本、允许动作与拒绝原因，发放一次性确认令牌。"""
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=int(payload["ttl_seconds"])))
        operation = payload["operation"]
        parameters = {"priority": payload.get("priority")}
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            resolved = self._resolve_selector(repository, payload["selector"])
            items: list[dict[str, Any]] = []
            for position, (task_id, task) in enumerate(resolved):
                if task is None:
                    items.append({
                        "position": position, "task_id": task_id, "version": None,
                        "status": None, "allowed": False, "allowed_actions": [],
                        "reject_reason": "任务不存在或已被删除",
                    })
                    continue
                allowed, actions, reject_reason = self._evaluate(task, operation)
                items.append({
                    "position": position, "task_id": int(task["id"]), "version": int(task["version"]),
                    "status": task["status"], "allowed": allowed,
                    "allowed_actions": actions, "reject_reason": reject_reason,
                })
            frozen_ids = [item["task_id"] for item in items]
            selector_record = {
                "criteria": payload["selector"],
                "frozen_task_ids": frozen_ids,
            }
            selector_digest = digest(selector_record)
            summary_digest = digest({
                "operation": operation,
                "selector_digest": selector_digest,
                "parameters": parameters,
                "items": [
                    {"task_id": item["task_id"], "version": item["version"],
                     "status": item["status"], "allowed": item["allowed"]}
                    for item in items
                ],
            })
            token = secrets.token_hex(16)
            allowed_count = sum(1 for item in items if item["allowed"])
            preview_id = repository.create_preview(
                token=token, operation=operation, selector=selector_record,
                selector_digest=selector_digest, parameters=parameters,
                actor=payload["actor"], reason=payload["reason"],
                summary_digest=summary_digest, item_count=len(items),
                allowed_count=allowed_count, rejected_count=len(items) - allowed_count,
                expires_at=expires, now=now,
            )
            for item in items:
                repository.add_preview_item(
                    preview_id=preview_id, task_id=item["task_id"], task_version=item["version"],
                    status_at_preview=item["status"], allowed=item["allowed"],
                    allowed_actions=item["allowed_actions"], reject_reason=item["reject_reason"],
                    position=item["position"],
                )
        return {
            "token": token,
            "operation": operation,
            "actor": payload["actor"],
            "reason": payload["reason"],
            "parameters": parameters,
            "summary_digest": summary_digest,
            "selector_digest": selector_digest,
            "created_at": now,
            "expires_at": expires,
            "ttl_seconds": int(payload["ttl_seconds"]),
            "counts": {
                "total": len(items),
                "allowed": allowed_count,
                "rejected": len(items) - allowed_count,
            },
            "items": [{k: v for k, v in item.items() if k != "position"} for item in items],
        }

    def confirm_batch_preview(self, payload: dict[str, Any]) -> dict[str, Any]:
        """凭一次性令牌执行预演过的批量操作；全有或全无，或显式接受逐条结果。"""
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            preview = repository.preview_by_token(payload["token"])
            if preview is None:
                raise NotFoundError("预演令牌不存在，请重新发起预演")

            # 同一令牌重复使用：不得重复操作，直接回放首次批次结果。
            if preview["status"] == "confirmed":
                return self._batch_result_response(repository, preview, replayed=True)

            stored_items = [dict(row) for row in repository.preview_items(preview["id"])]
            operation = preview["operation"]
            parameters = json.loads(preview["parameters_json"])
            diffs = self._collect_drifts(repository, stored_items, operation)
            expired = now > preview["expires_at"]
            if expired:
                raise ConflictError(
                    "预演令牌已过期，请重新预演后确认",
                    context={"code": "preview_expired", "expires_at": preview["expires_at"], "diffs": diffs},
                )

            if not secrets.compare_digest(preview["summary_digest"], payload["summary_digest"]):
                raise ConflictError(
                    "预演摘要校验失败，选择条件或条目可能已被篡改，请重新预演",
                    context={"code": "summary_mismatch"},
                )

            all_or_nothing = payload["execution_mode"] == "all_or_nothing"

            # 全有或全无：预演时就存在不可执行条目时，除非显式要求再次尝试，否则整批拒绝。
            if all_or_nothing and not payload["include_rejected"] and int(preview["rejected_count"]) > 0:
                raise ConflictError(
                    "预演中存在不可执行条目，无法按全有或全无策略确认",
                    context={
                        "code": "preview_has_rejections",
                        "execution_mode": "all_or_nothing",
                        "rejected_count": int(preview["rejected_count"]),
                    },
                )

            # 全有或全无：任何预演时允许的条目发生漂移，整批放弃（事务回滚）。
            blocking = [d for d in diffs if d["preview_allowed"]]
            if all_or_nothing and blocking:
                raise ConflictError(
                    "检测到任务状态漂移，按全有或全无策略整批未执行",
                    context={"code": "drift_detected", "execution_mode": "all_or_nothing", "diffs": diffs},
                )

            succeeded: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []
            failed: list[dict[str, Any]] = []
            drift_by_task = {d["task_id"]: d for d in diffs}

            for stored in stored_items:
                task_id = stored["task_id"]
                drift = drift_by_task.get(task_id)
                task = repository.task_by_id(task_id) if task_id is not None else None

                if not stored["allowed"]:
                    # 预演时拒绝的条目默认跳过并保留拒绝原因。
                    if not payload["include_rejected"]:
                        skipped.append(self._skipped_entry(stored, drift, reason_code="rejected_at_preview", reason=stored["reject_reason"]))
                        continue
                    # 显式要求重试拒绝条目：基于当前版本与状态重新评估。
                    if task is None:
                        skipped.append(self._skip_from_drift(stored, drift))
                        if all_or_nothing:
                            raise ConflictError(
                                "包含预演拒绝条目后仍有任务不可执行，按全有或全无策略整批未执行",
                                context={"code": "still_rejected", "execution_mode": "all_or_nothing", "diffs": diffs},
                            )
                        continue
                    now_allowed, _, _ = self._evaluate(task, operation)
                    if not now_allowed:
                        if all_or_nothing:
                            raise ConflictError(
                                "包含预演拒绝条目后仍有任务不可执行，按全有或全无策略整批未执行",
                                context={"code": "still_rejected", "execution_mode": "all_or_nothing", "task_id": task_id, "diffs": diffs},
                            )
                        skipped.append(self._skipped_entry(
                            stored, drift, reason_code="rejected_at_preview", reason=stored["reject_reason"],
                        ))
                        continue
                elif drift is not None or task is None:
                    # 预演时允许但已漂移：部分模式下跳过并给出差异。
                    skipped.append(self._skip_from_drift(stored, drift))
                    continue

                try:
                    after = self._apply_confirmed_mutation(
                        repository, connection, task, operation=operation,
                        parameters=parameters, actor=payload["confirmed_by"],
                        reason=preview["reason"], batch_key=preview["token"], now=now,
                    )
                except ConflictError as exc:
                    failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
                    if all_or_nothing:
                        raise
                    continue
                succeeded.append({
                    "task_id": task_id, "status": after["status"],
                    "version": after["version"], "priority": after["priority"],
                })

            result_id = repository.create_batch_result(
                preview_id=preview["id"], token=preview["token"], operation=operation,
                execution_mode=payload["execution_mode"], actor=payload["confirmed_by"],
                reason=preview["reason"], succeeded=succeeded, skipped=skipped, failed=failed, now=now,
            )
            repository.mark_preview_confirmed(
                preview_id=preview["id"], confirmed_by=payload["confirmed_by"],
                confirmed_at=now, execution_mode=payload["execution_mode"], batch_result_id=result_id,
            )
            preview = repository.preview_by_token(payload["token"])
            return self._batch_result_response(repository, preview, replayed=False, diffs=diffs)

    def get_batch_preview(self, token: str) -> dict[str, Any]:
        """查看预演（含条目实时状态），不会执行任何操作。"""
        repository = self.repository
        preview = repository.preview_by_token(token)
        if preview is None:
            raise NotFoundError("预演令牌不存在")
        now = to_storage(self.clock.now())
        items = [dict(row) for row in repository.preview_items(preview["id"])]
        response_items: list[dict[str, Any]] = []
        for stored in items:
            current = repository.task_by_id(stored["task_id"]) if stored["task_id"] is not None else None
            response_items.append({
                "task_id": stored["task_id"],
                "preview_version": stored["task_version"],
                "preview_status": stored["status_at_preview"],
                "allowed": bool(stored["allowed"]),
                "allowed_actions": json.loads(stored["allowed_actions_json"]),
                "reject_reason": stored["reject_reason"],
                "current_version": None if current is None else int(current["version"]),
                "current_status": None if current is None else current["status"],
                "changed": current is None or int(current["version"]) != int(stored["task_version"]),
            })
        return {
            "token": preview["token"],
            "operation": preview["operation"],
            "selector": json.loads(preview["selector_json"]),
            "parameters": json.loads(preview["parameters_json"]),
            "actor": preview["actor"],
            "reason": preview["reason"],
            "summary_digest": preview["summary_digest"],
            "status": preview["status"],
            "confirmed_by": preview["confirmed_by"] or None,
            "confirmed_at": preview["confirmed_at"],
            "execution_mode": preview["execution_mode"] or None,
            "created_at": preview["created_at"],
            "expires_at": preview["expires_at"],
            "expired": now > preview["expires_at"],
            "counts": {
                "total": preview["item_count"],
                "allowed": preview["allowed_count"],
                "rejected": preview["rejected_count"],
            },
            "items": response_items,
        }

    def get_batch_review(self, token: str) -> dict[str, Any]:
        """重建批次：预演选择、确认人、每条任务为何被修改或跳过，以及干预前后快照。"""
        repository = self.repository
        preview = repository.preview_by_token(token)
        if preview is None:
            raise NotFoundError("预演令牌不存在")
        stored_items = [dict(row) for row in repository.preview_items(preview["id"])]
        result_row = repository.batch_result_by_preview(preview["id"])
        result = json.loads(result_row["succeeded_json"]) if result_row else []
        skipped = json.loads(result_row["skipped_json"]) if result_row else []
        failed = json.loads(result_row["failed_json"]) if result_row else []
        outcome_by_task: dict[int, dict[str, Any]] = {}
        for entry in result:
            outcome_by_task[entry["task_id"]] = {"outcome": "succeeded", **entry}
        for entry in skipped:
            outcome_by_task[entry["task_id"]] = {"outcome": "skipped", **entry}
        for entry in failed:
            outcome_by_task[entry["task_id"]] = {"outcome": "failed", **entry}
        interventions = repository.interventions_by_batch(token)
        intervention_by_task = {item["task_id"]: item for item in interventions}

        trail_items: list[dict[str, Any]] = []
        for stored in stored_items:
            task_id = stored["task_id"]
            intervention = intervention_by_task.get(task_id)
            trail_items.append({
                "task_id": task_id,
                "preview": {
                    "version": stored["task_version"],
                    "status": stored["status_at_preview"],
                    "allowed": bool(stored["allowed"]),
                    "allowed_actions": json.loads(stored["allowed_actions_json"]),
                    "reject_reason": stored["reject_reason"],
                },
                "execution": outcome_by_task.get(task_id, {"outcome": "not_executed"}),
                "intervention": None
                if intervention is None
                else {
                    "id": intervention["id"],
                    "actor": intervention["actor"],
                    "action": intervention["action"],
                    "reason": intervention["reason"],
                    "before": _snapshot(json.loads(intervention["before_json"])),
                    "after": _snapshot(json.loads(intervention["after_json"])),
                    "created_at": intervention["created_at"],
                },
            })
        return {
            "token": preview["token"],
            "operation": preview["operation"],
            "selector": json.loads(preview["selector_json"]),
            "parameters": json.loads(preview["parameters_json"]),
            "preview": {
                "actor": preview["actor"],
                "reason": preview["reason"],
                "summary_digest": preview["summary_digest"],
                "created_at": preview["created_at"],
                "expires_at": preview["expires_at"],
                "counts": {
                    "total": preview["item_count"],
                    "allowed": preview["allowed_count"],
                    "rejected": preview["rejected_count"],
                },
            },
            "confirmation": None
            if result_row is None
            else {
                "confirmed_by": result_row["actor"],
                "execution_mode": result_row["execution_mode"],
                "reason": result_row["reason"],
                "confirmed_at": result_row["created_at"],
                "counts": {
                    "succeeded": len(result),
                    "skipped": len(skipped),
                    "failed": len(failed),
                },
            },
            "items": trail_items,
        }

    # ------------------------------------------------------------------
    # 两阶段操作的辅助方法
    # ------------------------------------------------------------------

    def _resolve_selector(self, repository: ComputeRepository, selector: dict[str, Any]) -> list[tuple[int | None, sqlite3.Row | dict[str, Any] | None]]:
        if selector.get("task_ids"):
            ordered_ids = list(dict.fromkeys(selector["task_ids"]))
            rows = repository.tasks_by_ids(ordered_ids)
            by_id = {int(row["id"]): row for row in rows}
            return [(task_id, by_id.get(task_id)) for task_id in ordered_ids]
        criteria = selector["filter"]
        tasks = repository.list_tasks(
            status=criteria.get("status"), project_code=criteria.get("project_code"),
            requested_by=criteria.get("requested_by"), limit=int(criteria.get("limit") or 500),
        )
        return [(int(task["id"]), task) for task in tasks]

    @staticmethod
    def _evaluate(task: sqlite3.Row, operation: str) -> tuple[bool, list[str], str]:
        status = task["status"]
        actions = allowed_actions_for_status(status)
        if status in BATCH_ALLOWED_STATUSES[operation]:
            return True, actions, ""
        return False, actions, _reject_reason(operation, status)

    @staticmethod
    def _collect_drifts(repository: ComputeRepository, stored_items: list[dict[str, Any]], operation: str) -> list[dict[str, Any]]:
        diffs: list[dict[str, Any]] = []
        for stored in stored_items:
            task_id = stored["task_id"]
            if task_id is None:
                continue
            current = repository.task_by_id(task_id)
            diff: dict[str, Any] | None = None
            if current is None:
                diff = {
                    "task_id": task_id, "kind": "task_missing",
                    "preview_allowed": bool(stored["allowed"]),
                    "preview_version": stored["task_version"], "current_version": None,
                    "preview_status": stored["status_at_preview"], "current_status": None,
                    "detail": "任务已不存在",
                }
            elif int(current["version"]) != int(stored["task_version"]):
                kind = "state_no_longer_allowed" if (
                    stored["allowed"] and current["status"] not in BATCH_ALLOWED_STATUSES[operation]
                ) else "version_changed"
                diff = {
                    "task_id": task_id, "kind": kind,
                    "preview_allowed": bool(stored["allowed"]),
                    "preview_version": stored["task_version"], "current_version": int(current["version"]),
                    "preview_status": stored["status_at_preview"], "current_status": current["status"],
                    "detail": f"状态 {stored['status_at_preview']}(v{stored['task_version']}) → {current['status']}(v{current['version']})",
                }
            if diff is not None:
                diffs.append(diff)
        return diffs

    @staticmethod
    def _skipped_entry(stored: dict[str, Any], drift: dict[str, Any] | None, *, reason_code: str, reason: str) -> dict[str, Any]:
        if drift is None:
            return {
                "task_id": stored["task_id"], "reason_code": reason_code, "reason": reason,
                "preview_version": stored["task_version"], "current_version": stored["task_version"],
                "preview_status": stored["status_at_preview"], "current_status": stored["status_at_preview"],
            }
        return {
            "task_id": drift["task_id"], "reason_code": reason_code, "reason": reason,
            "preview_version": drift["preview_version"], "current_version": drift["current_version"],
            "preview_status": drift["preview_status"], "current_status": drift["current_status"],
        }

    @staticmethod
    def _skip_from_drift(stored: dict[str, Any], drift: dict[str, Any] | None) -> dict[str, Any]:
        if drift is None:
            return {
                "task_id": stored["task_id"], "reason_code": "task_missing",
                "reason": "任务不存在或已被删除",
                "preview_version": stored["task_version"], "current_version": None,
                "preview_status": stored["status_at_preview"], "current_status": None,
            }
        return {
            "task_id": drift["task_id"], "reason_code": drift["kind"],
            "reason": drift["detail"],
            "preview_version": drift["preview_version"], "current_version": drift["current_version"],
            "preview_status": drift["preview_status"], "current_status": drift["current_status"],
        }

    def _apply_confirmed_mutation(
        self, repository: ComputeRepository, connection: sqlite3.Connection, task: sqlite3.Row, *,
        operation: str, parameters: dict[str, Any], actor: str, reason: str, batch_key: str, now: str,
    ) -> dict[str, Any]:
        """按预演版本执行条件更新；版本或状态已漂移时更新行数为 0，交由上层跳过。"""
        before = dict(task)
        task_id = int(task["id"])
        expected_version = int(task["version"])
        if operation == "cancel":
            cursor = connection.execute(
                "UPDATE compute_tasks SET status=CASE WHEN status='running' THEN 'cancel_requested' ELSE 'cancelled' END,"
                "finished_at=CASE WHEN status='running' THEN NULL ELSE ? END,updated_at=?,version=version+1 "
                "WHERE id=? AND version=? AND status IN ('queued','running')",
                (now, now, task_id, expected_version),
            )
        elif operation == "retry":
            chosen = task["priority"] if parameters.get("priority") is None else int(parameters["priority"])
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',"
                "finished_at=NULL,updated_at=?,version=version+1 WHERE id=? AND version=? AND status IN ('failed','cancelled')",
                (chosen, now, now, task_id, expected_version),
            )
        else:
            cursor = connection.execute(
                "UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=? AND version=? AND status IN ('queued','running')",
                (int(parameters["priority"]), now, task_id, expected_version),
            )
        if cursor.rowcount != 1:
            raise ConflictError("任务版本或状态在执行瞬间发生变化，已跳过")
        after = dict(repository.task_by_id(task_id))
        repository.add_intervention(
            task_id=task_id, actor=actor, action=operation, reason=reason,
            before=before, after=after, batch_key=batch_key, now=now,
        )
        return after

    def _batch_result_response(
        self, repository: ComputeRepository, preview: sqlite3.Row, *, replayed: bool, diffs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        result_row = repository.batch_result_by_preview(preview["id"])
        succeeded = json.loads(result_row["succeeded_json"]) if result_row else []
        skipped = json.loads(result_row["skipped_json"]) if result_row else []
        failed = json.loads(result_row["failed_json"]) if result_row else []
        return {
            "token": preview["token"],
            "operation": preview["operation"],
            "status": preview["status"],
            "execution_mode": preview["execution_mode"],
            "confirmed_by": preview["confirmed_by"],
            "confirmed_at": preview["confirmed_at"],
            "replayed": replayed,
            "summary_digest": preview["summary_digest"],
            "counts": {
                "succeeded": len(succeeded),
                "skipped": len(skipped),
                "failed": len(failed),
            },
            "succeeded": succeeded,
            "skipped": skipped,
            "failed": failed,
            **({"diffs": diffs} if diffs else {}),
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
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

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
