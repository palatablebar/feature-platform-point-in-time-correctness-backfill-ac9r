"""版本化回填写入层：按「特征版本生效时间 + 样本标签时间」判定候选值何时生效。

口径修正后的定向回填只替换被点名的特征列，其他已确认特征值不参与重算。
本模块保证：

1. **生效判定**：候选特征版本 ``effective_from <= label_time`` 才可能写入；
   版本在标签时间之后才生效（旧回填迟到、口径已修正）时保留样本当前值，
   结果记 ``skipped_stale``；若当前值来自更新版本，再叠加 ``is_conflict``。
2. **只替换特征列**：写回只改 ``sample_row.features_json[feature_code]`` 与
   该列的 ``feature_provenance`` 溯源，其他特征键原样保留——训练集无需整行重跑。
3. **幂等**：同一 ``(backfill_id, sample_row_id, feature_code)`` 已存在结果时
   不新增、不覆盖（``skipped_duplicate``），数据库唯一约束兜底并发。

判定主函数 :func:`decide_apply` 是纯函数，可独立自测。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.constants.enums import BackfillResultStatus
from src.models import BackfillResult, BackfillTask, EntityEvent, FeatureDef, SampleJob, SampleRow
from src.services import feature_service
from src.services.pit_engine import (
    Event,
    FeatureSpec,
    PitFailure,
    aggregate,
    build_index,
    matched_events,
    normalize_events,
)
from src.utils.timefmt import ensure_utc, iso_z, utcnow

# 与 sample_service / backfill_service 保持一致的事件回看上限
MAX_LOOKBACK_SECONDS = 30 * 86400


# --------------------------------------------------------------------------- #
# 纯函数：生效判定（不碰数据库，可自测）
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Candidate:
    """一次回填对「样本 × 特征」算出的候选值。"""

    value: float
    version_no: int
    effective_from: datetime


@dataclass(frozen=True)
class Decision:
    status: str
    is_conflict: bool
    reason: str


def decide_apply(candidate: Candidate, label_time: datetime, current_version_no: int | None) -> Decision:
    """判定候选值对单个样本特征列的生效方式。

    判定顺序：

    * 候选版本生效时间晚于标签时间 → ``skipped_stale``（旧结果迟到，当前值保留）；
      样本当前值版本比候选更新时，额外标记冲突，便于页面核对「新值曾被旧值威胁」。
    * 样本当前值版本新于候选版本 → ``skipped_stale`` + 冲突：旧口径任务晚于新口径
      任务到达，即使该旧版本在标签时间适用，也不得把当前值改回旧值；
    * 样本当前值版本 == 候选版本 → ``retained``（同版本已落值，重复结果不回退）；
    * 其余情况 → ``replaced``（候选版本在标签时间已生效且更新，替换该特征列）。
    """
    effective_from = ensure_utc(candidate.effective_from)
    label_time = ensure_utc(label_time)
    if effective_from > label_time:
        conflict = current_version_no is not None and current_version_no > candidate.version_no
        return Decision(
            BackfillResultStatus.SKIPPED_STALE.value,
            conflict,
            f"特征版本 v{candidate.version_no} 生效于 {iso_z(candidate.effective_from)}，"
            f"晚于样本标签时间 {iso_z(label_time)}，保留当前值"
            + ("（当前值来自更新版本，记为冲突）" if conflict else ""),
        )
    if current_version_no is not None and current_version_no > candidate.version_no:
        return Decision(
            BackfillResultStatus.SKIPPED_STALE.value,
            True,
            f"旧口径 v{candidate.version_no} 回填迟到：样本当前值已来自更新版本 "
            f"v{current_version_no}，保留当前值并记为冲突",
        )
    if current_version_no is not None and current_version_no == candidate.version_no:
        return Decision(
            BackfillResultStatus.RETAINED.value,
            False,
            f"样本当前值版本 v{current_version_no} 与候选版本相同，保留当前值",
        )
    return Decision(
        BackfillResultStatus.REPLACED.value,
        False,
        f"候选版本 v{candidate.version_no} 在标签时间已生效且新于当前版本"
        f" v{current_version_no if current_version_no is not None else '∅'}，替换该特征列",
    )


# --------------------------------------------------------------------------- #
# 数据装载
# --------------------------------------------------------------------------- #
def _load_events(db: Session, entity_keys: Sequence[str], range_end: datetime) -> list[Event]:
    """装载回填所需事件：与样本构建同一回看上限，右端不超过回填范围终点。"""
    floor = range_end - timedelta(seconds=MAX_LOOKBACK_SECONDS)
    rows = list(
        db.scalars(
            select(EntityEvent)
            .where(
                EntityEvent.entity_key.in_(entity_keys),
                EntityEvent.event_ts >= floor,
                EntityEvent.event_ts <= range_end,
            )
            .order_by(EntityEvent.event_ts, EntityEvent.id)
        )
    )
    return normalize_events(
        [
            {
                "id": row.id,
                "entity_key": row.entity_key,
                "event_type": row.event_type,
                "ts": row.event_ts,
                "amount": float(row.amount),
            }
            for row in rows
        ]
    )


def _target_sample_rows(db: Session, task: BackfillTask) -> list[SampleRow]:
    """命中回填范围、且所属样本任务包含目标特征的样本行。"""
    feature_ids = list(task.feature_ids or [])
    if feature_ids:
        job_ids = {
            job.id
            for job in db.scalars(select(SampleJob))
            if set(feature_ids) & set(job.feature_ids or [])
        }
        if not job_ids:
            return []
        stmt = (
            select(SampleRow)
            .where(
                SampleRow.job_id.in_(job_ids),
                SampleRow.label_time >= task.range_start,
                SampleRow.label_time <= task.range_end,
            )
            .order_by(SampleRow.id)
        )
    else:
        stmt = (
            select(SampleRow)
            .where(
                SampleRow.label_time >= task.range_start,
                SampleRow.label_time <= task.range_end,
            )
            .order_by(SampleRow.id)
        )
    return list(db.scalars(stmt))


def _lock_feature_versions(db: Session, task: BackfillTask) -> dict[int, FeatureDef]:
    """任务发起/执行时锁定目标特征定义；空 feature_ids 表示回填全部已发布特征。"""
    if task.feature_ids:
        features = list(db.scalars(select(FeatureDef).where(FeatureDef.id.in_(task.feature_ids))))
    else:
        features = list(db.scalars(select(FeatureDef)))
    return {feature.id: feature for feature in features}


# --------------------------------------------------------------------------- #
# 主流程：执行定向回填
# --------------------------------------------------------------------------- #
def apply_backfill(db: Session, task: BackfillTask) -> dict:
    """计算候选值并按版本规则写回，返回逐状态计数与结果行。

    前置幂等：任务已有任意 ``BackfillResult`` 时整体跳过（重复运行不新增、
    不覆盖），任务状态置 ``skipped``。
    """
    existing = db.scalar(select(BackfillResult.id).where(BackfillResult.backfill_id == task.id).limit(1))
    if existing is not None:
        task.status = "skipped"
        db.commit()
        return {"skipped_duplicate_task": True, "results": []}

    features = _lock_feature_versions(db, task)
    if not features:
        raise PitFailure("no_feature", "回填任务没有可用的特征定义", {"backfill_id": task.id})

    # 任务锁定版本（发起时记录；兼容旧任务则现场取最新版本号）
    locked_versions: dict[int, int] = {
        fid: int((task.feature_versions or {}).get(str(fid)) or feature.version)
        for fid, feature in features.items()
    }
    task.feature_versions = {str(fid): version_no for fid, version_no in locked_versions.items()}

    sample_rows = _target_sample_rows(db, task)
    if not sample_rows:
        raise PitFailure(
            "insufficient_samples",
            "回填范围与目标特征没有交集样本行",
            {"backfill_id": task.id, "range": [iso_z(task.range_start), iso_z(task.range_end)]},
        )

    entity_keys = sorted({row.entity_key for row in sample_rows})
    events = _load_events(db, entity_keys, task.range_end)
    index_by_entity = build_index(events)

    # 每个目标特征取「任务锁定版本」的快照作为计算口径
    specs: dict[int, FeatureSpec] = {}
    effective: dict[int, datetime] = {}
    for fid, version_no in locked_versions.items():
        version = _get_version(db, fid, version_no)
        specs[fid] = feature_service.spec_from_version(version)
        effective[fid] = version.effective_from

    now = utcnow()
    results: list[BackfillResult] = []
    counters = {status.value: 0 for status in BackfillResultStatus}
    conflicts = 0

    # 一次性载入本任务已有的结果键，重复运行（或并发）时整格跳过，不新增不覆盖
    existing_keys = {
        (row.sample_row_id, row.feature_code)
        for row in db.scalars(select(BackfillResult).where(BackfillResult.backfill_id == task.id))
    }

    for row in sample_rows:
        provenance = dict(row.feature_provenance or {})
        features_json = dict(row.features_json or {})
        index = index_by_entity.get(row.entity_key)
        row_dirty = False
        # 归一化：SQLite 等无 tz 存储读回为 naive，统一按 UTC 处理（PG 下为幂等转换）
        label_time = ensure_utc(row.label_time)

        for fid, spec in specs.items():
            if (row.id, spec.code) in existing_keys:
                counters[BackfillResultStatus.SKIPPED_DUPLICATE.value] += 1
                continue

            selected = matched_events(spec, index, label_time)
            candidate_value = aggregate(spec.aggregate, selected, label_time)
            current_version_no = (provenance.get(spec.code) or {}).get("version_no")
            candidate = Candidate(
                value=float(candidate_value),
                version_no=locked_versions[fid],
                effective_from=ensure_utc(effective[fid]),
            )
            decision = decide_apply(candidate, label_time, current_version_no)
            previous_value = features_json.get(spec.code)

            result = BackfillResult(
                backfill_id=task.id,
                sample_row_id=row.id,
                job_id=row.job_id,
                label_row_id=row.label_row_id,
                entity_key=row.entity_key,
                label_time=row.label_time,
                feature_id=fid,
                feature_code=spec.code,
                candidate_value=candidate.value,
                candidate_version_no=candidate.version_no,
                effective_from=candidate.effective_from,
                previous_value=(float(previous_value) if previous_value is not None else None),
                previous_version_no=current_version_no,
                status=decision.status,
                is_conflict=decision.is_conflict,
                decision_reason=decision.reason,
                applied_at=now if decision.status == BackfillResultStatus.REPLACED.value else None,
            )
            db.add(result)
            results.append(result)
            counters[decision.status] += 1
            if decision.is_conflict:
                conflicts += 1

            if decision.status == BackfillResultStatus.REPLACED.value:
                # 只替换被点名的特征列，其他特征键原样保留
                features_json[spec.code] = candidate.value
                provenance[spec.code] = {
                    "version_no": candidate.version_no,
                    "backfill_task_id": task.id,
                    "applied_at": iso_z(now),
                }
                row_dirty = True

        if row_dirty:
            row.features_json = features_json
            row.feature_provenance = provenance

    db.commit()
    return {
        "skipped_duplicate_task": False,
        "results": results,
        "counters": counters,
        "conflict_count": conflicts,
        "sample_row_count": len(sample_rows),
    }


def _get_version(db: Session, feature_id: int, version_no: int):
    from src.models import FeatureVersion

    version = db.scalar(
        select(FeatureVersion).where(
            FeatureVersion.feature_id == feature_id,
            FeatureVersion.version_no == version_no,
        )
    )
    if version is None:
        # 兜底：锁定版本缺失时退到该特征的最新版本
        version = feature_service.latest_version(db, feature_id)
    return version


# --------------------------------------------------------------------------- #
# 查询：结果页核对
# --------------------------------------------------------------------------- #
def list_results(
    db: Session,
    backfill_id: int,
    status: str | None = None,
    conflict_only: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[BackfillResult], int]:
    stmt = select(BackfillResult).where(BackfillResult.backfill_id == backfill_id)
    count_stmt = select(BackfillResult.id).where(BackfillResult.backfill_id == backfill_id)
    if status:
        stmt = stmt.where(BackfillResult.status == status)
        count_stmt = count_stmt.where(BackfillResult.status == status)
    if conflict_only:
        stmt = stmt.where(BackfillResult.is_conflict.is_(True))
        count_stmt = count_stmt.where(BackfillResult.is_conflict.is_(True))
    total = len(list(db.scalars(count_stmt)))
    rows = list(
        db.scalars(stmt.order_by(BackfillResult.id).limit(limit).offset(offset))
    )
    return rows, total


def summarize_results(db: Session, backfill_id: int) -> dict:
    rows = list(db.scalars(select(BackfillResult).where(BackfillResult.backfill_id == backfill_id)))
    by_status: dict[str, int] = {}
    conflict_count = 0
    affected_sample_rows = set()
    for row in rows:
        by_status[row.status] = by_status.get(row.status, 0) + 1
        if row.is_conflict:
            conflict_count += 1
        if row.status == BackfillResultStatus.REPLACED.value:
            affected_sample_rows.add(row.sample_row_id)
    return {
        "total": len(rows),
        "by_status": by_status,
        "conflict_count": conflict_count,
        "replaced_sample_rows": len(affected_sample_rows),
    }


# --------------------------------------------------------------------------- #
# 自测：`python -m src.services.backfill_apply`（只跑纯函数判定，不依赖数据库）
# --------------------------------------------------------------------------- #
def _self_test() -> int:
    import json

    label_time = datetime(2026, 3, 1, tzinfo=timezone.utc)
    cases = []

    # 1) 新版本在标签时间前生效、且新于当前版本 → replaced
    cases.append(
        decide_apply(
            Candidate(value=3.0, version_no=2, effective_from=label_time - timedelta(days=1)),
            label_time,
            current_version_no=1,
        ).status
        == BackfillResultStatus.REPLACED.value
    )
    # 2) 同版本结果到达 → retained，不回退
    cases.append(
        decide_apply(
            Candidate(value=3.0, version_no=2, effective_from=label_time - timedelta(days=1)),
            label_time,
            current_version_no=2,
        ).status
        == BackfillResultStatus.RETAINED.value
    )
    # 2b) 旧口径任务迟到（版本旧、当前已是更新版本，且生效时间在标签前）→ skipped_stale + 冲突
    late_older = decide_apply(
        Candidate(value=9.0, version_no=1, effective_from=label_time - timedelta(days=30)),
        label_time,
        current_version_no=2,
    )
    cases.append(
        late_older.status == BackfillResultStatus.SKIPPED_STALE.value
        and late_older.is_conflict is True
    )
    # 3) 旧版本迟到（生效时间晚于标签时间），当前值为更新版本 → skipped_stale + 冲突
    stale = decide_apply(
        Candidate(value=9.0, version_no=1, effective_from=label_time + timedelta(days=1)),
        label_time,
        current_version_no=2,
    )
    cases.append(stale.status == BackfillResultStatus.SKIPPED_STALE.value and stale.is_conflict is True)
    # 4) 旧版本迟到但样本还没有更新版本 → skipped_stale，不算冲突
    stale_no_current = decide_apply(
        Candidate(value=9.0, version_no=1, effective_from=label_time + timedelta(days=1)),
        label_time,
        current_version_no=None,
    )
    cases.append(
        stale_no_current.status == BackfillResultStatus.SKIPPED_STALE.value
        and stale_no_current.is_conflict is False
    )
    # 5) 生效时刻恰好等于标签时间（<= 判定）→ 适用，走 replaced
    cases.append(
        decide_apply(
            Candidate(value=1.0, version_no=2, effective_from=label_time),
            label_time,
            current_version_no=1,
        ).status
        == BackfillResultStatus.REPLACED.value
    )

    ok = all(cases)
    print(
        json.dumps(
            {
                "self_test": "backfill_apply",
                "cases": len(cases),
                "passed": sum(1 for case in cases if case),
                "ok": ok,
            },
            ensure_ascii=False,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    import sys
    from datetime import timezone

    sys.exit(_self_test())
