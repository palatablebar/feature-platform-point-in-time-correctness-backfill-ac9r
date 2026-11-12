"""Backfill handlers (wired by app/routes/backfills.py)."""
from src.middlewares import quota as quota_middleware
from src.middlewares.audit_logger import record
from src.schemas.common import page
from src.services import backfill_apply, backfill_service
from src.utils.serialize import backfill_result_out


def list_backfills(db, user) -> dict:
    items = [backfill_service.out(task) for task in backfill_service.list_backfills(db)]
    return page(items, len(items))


def create_backfill(db, user, payload, request=None) -> dict:
    task = backfill_service.create_backfill(db, payload, user)
    record(
        request,
        "backfill_create",
        "backfill_task",
        task.id,
        after={"code": task.code, "mode": task.mode},
    )
    return backfill_service.out(task, with_report=True)


def get_backfill(db, user, backfill_id: int) -> dict:
    task = backfill_service.get_backfill(db, backfill_id)
    payload = backfill_service.out(task, with_report=True)
    payload["apply_summary"] = backfill_apply.summarize_results(db, backfill_id)
    return payload


def list_backfill_results(db, user, backfill_id: int, status: str | None, conflict_only: bool) -> dict:
    # 先校验任务存在（不存在抛 404）
    backfill_service.get_backfill(db, backfill_id)
    rows, total = backfill_apply.list_results(
        db, backfill_id, status=status, conflict_only=conflict_only, limit=200
    )
    summary = backfill_apply.summarize_results(db, backfill_id)
    return {
        "items": [backfill_result_out(row) for row in rows],
        "total": total,
        "summary": summary,
    }


def run_backfill(db, user, backfill_id: int, request=None) -> dict:
    task = backfill_service.get_backfill(db, backfill_id)
    quota_middleware.consume_daily_quota(db, user, "backfill_run")
    record(request, "backfill_run", "backfill_task", task.id, after={"code": task.code})
    from src.tasks.backfill import dispatch_backfill

    return dispatch_backfill(task.id, user.email)
