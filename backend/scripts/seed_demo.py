"""幂等演示数据（seed）。

模拟客户：岚舟科技 · 风控算法组（9 人），消费贷模型特征与训练样本。

* 3 个账户实体 × 400 条事件 = 1200 条事件（申请/还款/登录，90 天）；
* 4 个特征定义：``last_7d_amount``（7d/strict/last）、``cnt_30d_apply``
  （30d/**inclusive**/count）、``max_1d_amount``（1d/**inclusive**/max）、
  ``days_since_last_apply``（30d/strict/recency_days）——其中两个 inclusive
  就是「边界误用」的现场；
* 2 个标签集各 400 样本；1 个已完成 sample_job（真跑 pit_engine，**真含 2 处泄漏**）
  + 1 个 failed ``bad_timestamp``（参数快照里的时间戳非法）；
* 1 个 backfill 任务（真跑 agg_engine，full == incremental）。

幂等：全部按自然键 upsert，已存在的行不覆盖（用户改动优先）；结束时打印
**恰好一行** ``SEED_SUMMARY accounts=3 events=1200 features=4 label_sets=2 \
jobs=2 samples=800 backfills=3 backfill_results=<N> users=3``，两次运行逐字一致。

版本化回填故事线（``backfill_result`` 台账可核对）：

* ``BF-SEED-002`` 用 strict 修正版 v2 定向回填 ``cnt_30d_apply`` → 受影响样本
  特征列 ``replaced``，其他特征列不动；
* ``BF-SEED-003`` 用旧 inclusive v1 口径迟到回填同一范围 → 当前值保留并记
  ``skipped_stale``，已被 v2 替换的样本同时标 ``is_conflict=true``；
* 对 ``BF-SEED-002`` 再跑一次 → 整任务 ``skipped``，不新增不覆盖。
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.config.database import SessionLocal
from src.constants.enums import BoundaryPolicy, EventType, FeatureStatus, Role
from src.models import (
    AuditLog,  # noqa: F401  (imported so create_all sees every table)
    BackfillResult,
    BackfillTask,
    EntityEvent,
    FeatureDef,
    FeatureVersion,
    LabelRow,
    LabelSet,
    SampleJob,
    SampleRow,
    User,
)
from src.services import backfill_service, sample_service
from src.utils.logger import log_event
from src.utils.password import DEMO_PASSWORD, hash_password
from src.utils.timefmt import iso_z, utcnow

RNG_SEED = 20261110
ACCOUNTS = ["ACC-LZ-0001", "ACC-LZ-0002", "ACC-LZ-0003"]
EVENT_TOTAL = 1200
EVENTS_PER_ACCOUNT = 400
LABEL_ROWS_PER_SET = 400
EVENT_DAYS = 90
LABEL_DAYS = 45

USERS = [
    ("owner@featpoint.demo", "陆知远 · 特征负责人", Role.OWNER, 20),
    ("analyst@featpoint.demo", "沈可 · 算法工程师", Role.ANALYST, 12),
    ("viewer@featpoint.demo", "温岳 · 风控审计", Role.VIEWER, 0),
]

FEATURES = [
    {
        "code": "last_7d_amount",
        "name": "近 7 天最近一笔申请金额",
        "description": "窗口右端排除标签时刻，取最近一笔申请金额（apply 且金额>0）",
        "event_type": EventType.APPLY.value,
        "amount_min": 0.01,
        "window_label": "7d",
        "window_seconds": 7 * 86400,
        "boundary_policy": BoundaryPolicy.STRICT.value,
        "aggregate_kind": "last",
    },
    {
        "code": "cnt_30d_apply",
        "name": "近 30 天申请次数",
        "description": "历史遗留口径：窗口右端写了 inclusive，会把标签时刻当刻的申请也算进来",
        "event_type": EventType.APPLY.value,
        "amount_min": None,
        "window_label": "30d",
        "window_seconds": 30 * 86400,
        "boundary_policy": BoundaryPolicy.INCLUSIVE.value,
        "aggregate_kind": "count",
    },
    {
        "code": "max_1d_amount",
        "name": "近 1 天最大还款金额",
        "description": "历史遗留口径：窗口右端写了 inclusive，边界上会吞掉标签时刻的还款",
        "event_type": EventType.REPAY.value,
        "amount_min": None,
        "window_label": "1d",
        "window_seconds": 86400,
        "boundary_policy": BoundaryPolicy.INCLUSIVE.value,
        "aggregate_kind": "max",
    },
    {
        "code": "days_since_last_apply",
        "name": "距最近一笔申请的天数",
        "description": "recency_days 派生聚合（last 的时间差），无事件记 -1",
        "event_type": EventType.APPLY.value,
        "amount_min": 0.01,
        "window_label": "30d",
        "window_seconds": 30 * 86400,
        "boundary_policy": BoundaryPolicy.STRICT.value,
        "aggregate_kind": "recency_days",
    },
]

LABEL_SETS = [
    ("LS-2026Q1-A", "2026Q1 申请样本 A（训练集）", "从申请流水抽取的 400 个样本键，标签=次月是否逾期"),
    ("LS-2026Q1-B", "2026Q1 申请样本 B（验证集）", "与 A 同分布的另一批 400 个样本键，用于回填一致性回归"),
]


# --------------------------------------------------------------------------- #
# 工具
# --------------------------------------------------------------------------- #
def _anchor() -> datetime:
    """以「运行日 UTC 零点」为锚生成确定性时间戳：同一自然日内重复执行完全一致。"""
    now = utcnow()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def _get_or_create(db: Session, model, defaults: dict, **kwargs):
    instance = db.scalar(select(model).filter_by(**kwargs))
    if instance is not None:
        return instance, False
    instance = model(**kwargs, **defaults)
    db.add(instance)
    db.flush()
    return instance, True


def _upsert_user(db: Session, email: str, display_name: str, role: Role, quota: int) -> User:
    user = db.scalar(select(User).where(User.email == email))
    if user is None:
        user = User(
            email=email,
            display_name=display_name,
            role=role.value,
            password_hash=hash_password(DEMO_PASSWORD, salt="featpoint-demo-salt"),
            daily_quota=quota,
            quota_used=0,
        )
        db.add(user)
        db.flush()
    return user


def _feature_snapshot(feature: FeatureDef) -> dict:
    return {
        "id": feature.id,
        "code": feature.code,
        "name": feature.name,
        "event_type": feature.event_type,
        "amount_min": float(feature.amount_min) if feature.amount_min is not None else None,
        "window_seconds": feature.window_seconds,
        "window_label": feature.window_label,
        "boundary_policy": feature.boundary_policy,
        "aggregate_kind": feature.aggregate_kind,
        "status": feature.status,
        "version": feature.version,
    }


def _ensure_feature(db: Session, spec: dict, owner: User, anchor: datetime) -> FeatureDef:
    # v1 口径对全部演示样本（90 天内）天然生效：生效时间锚定到数据窗口起点之前
    v1_effective = (anchor - timedelta(days=EVENT_DAYS + 1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    feature = db.scalar(select(FeatureDef).where(FeatureDef.code == spec["code"]))
    if feature is None:
        feature = FeatureDef(
            code=spec["code"],
            name=spec["name"],
            description=spec["description"],
            event_type=spec["event_type"],
            amount_min=spec["amount_min"],
            window_seconds=spec["window_seconds"],
            window_label=spec["window_label"],
            boundary_policy=spec["boundary_policy"],
            aggregate_kind=spec["aggregate_kind"],
            status=FeatureStatus.PUBLISHED.value,
            version=1,
            owner_id=owner.id,
            is_seed=True,
            published_at=utcnow(),
        )
        db.add(feature)
        db.flush()
        db.add(
            FeatureVersion(
                feature_id=feature.id,
                version_no=1,
                snapshot=_feature_snapshot(feature),
                change_note="seed 初始口径（含 2 处 inclusive 边界误用）",
                changed_by=owner.email,
                effective_from=v1_effective,
            )
        )
    elif db.scalar(
        select(func.count(FeatureVersion.id)).where(FeatureVersion.feature_id == feature.id)
    ) == 0:
        db.add(
            FeatureVersion(
                feature_id=feature.id,
                version_no=feature.version,
                snapshot=_feature_snapshot(feature),
                change_note="补记历史版本",
                changed_by=owner.email,
                effective_from=v1_effective,
            )
        )
    return feature


def _ensure_label_set(db: Session, code: str, name: str, description: str) -> LabelSet:
    label_set = db.scalar(select(LabelSet).where(LabelSet.code == code))
    if label_set is None:
        label_set = LabelSet(
            code=code, name=name, description=description, is_seed=True
        )
        db.add(label_set)
        db.flush()
    return label_set


def _ensure_label_rows(db: Session, label_set: LabelSet, rows: list[dict]) -> int:
    inserted = 0
    existing = {
        (row.entity_key, row.label_time)
        for row in db.scalars(select(LabelRow).where(LabelRow.label_set_id == label_set.id))
    }
    for row in rows:
        key = (row["entity_key"], row["label_time"])
        if key in existing:
            continue
        db.add(
            LabelRow(
                label_set_id=label_set.id,
                entity_key=row["entity_key"],
                label_time=row["label_time"],
                raw_label_time=iso_z(row["label_time"]),
                label=row["label"],
                is_seed=True,
            )
        )
        inserted += 1
    return inserted


def _ensure_events(db: Session, rows: list[dict]) -> int:
    inserted = 0
    existing = {
        (row.entity_key, row.event_type, row.event_ts)
        for row in db.scalars(select(EntityEvent))
    }
    for row in rows:
        key = (row["entity_key"], row["event_type"], row["event_ts"])
        if key in existing:
            continue
        db.add(
            EntityEvent(
                entity_key=row["entity_key"],
                event_type=row["event_type"],
                event_ts=row["event_ts"],
                amount=row["amount"],
                channel=row.get("channel", "app"),
                source_ref=row.get("source_ref", ""),
                is_seed=True,
            )
        )
        inserted += 1
    return inserted


# --------------------------------------------------------------------------- #
# 数据生成
# --------------------------------------------------------------------------- #
def _generate_labels(anchor: datetime) -> dict[str, list[dict]]:
    rng = random.Random(RNG_SEED + 7)
    out: dict[str, list[dict]] = {}
    for set_index, (code, _, _) in enumerate(LABEL_SETS):
        rows: list[dict] = []
        seen: set[tuple[str, datetime]] = set()
        for index in range(LABEL_ROWS_PER_SET):
            entity = ACCOUNTS[(index * 7 + set_index * 2) % len(ACCOUNTS)]
            offset = rng.randint(1, LABEL_DAYS * 86400 * 1_000_000)
            label_time = (anchor - timedelta(microseconds=offset)).replace(microsecond=0)
            if (entity, label_time) in seen:
                continue
            seen.add((entity, label_time))
            rows.append(
                {
                    "entity_key": entity,
                    "label_time": label_time,
                    "label": 1 if rng.random() < 0.31 else 0,
                }
            )
        # 保证恰好 400 行：不足时顺延补足
        guard = 0
        while len(rows) < LABEL_ROWS_PER_SET and guard < 5000:
            guard += 1
            entity = ACCOUNTS[guard % len(ACCOUNTS)]
            label_time = anchor - timedelta(days=rng.randint(1, LABEL_DAYS), minutes=guard, seconds=1)
            if (entity, label_time) in seen:
                continue
            seen.add((entity, label_time))
            rows.append(
                {"entity_key": entity, "label_time": label_time, "label": 1 if rng.random() < 0.31 else 0}
            )
        out[code] = rows
    return out


def _generate_events(anchor: datetime, labels: dict[str, list[dict]]) -> list[dict]:
    """1198 条随机事件 + 2 条「恰好等于标签时刻」的边界事件 = 1200 条。"""
    rng = random.Random(RNG_SEED + 11)
    rows: list[dict] = []
    seen: set[tuple[str, str, datetime]] = set()
    random_total = EVENT_TOTAL - 2
    guard = 0
    while len(rows) < random_total and guard < 40000:
        guard += 1
        entity = ACCOUNTS[guard % len(ACCOUNTS)]
        roll = rng.random()
        if roll < 0.45:
            event_type, amount = EventType.APPLY.value, round(rng.uniform(1000, 50000), 2)
        elif roll < 0.80:
            event_type, amount = EventType.REPAY.value, round(rng.uniform(200, 20000), 2)
        else:
            event_type, amount = EventType.LOGIN.value, 0.0
        offset = rng.randint(1, EVENT_DAYS * 86400 * 1_000_000)
        event_ts = anchor - timedelta(microseconds=offset)
        key = (entity, event_type, event_ts)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "entity_key": entity,
                "event_type": event_type,
                "event_ts": event_ts,
                "amount": amount,
                "channel": rng.choice(["app", "h5", "api"]),
                "source_ref": f"EVT-{len(rows) + 1:05d}",
            }
        )

    # 边界事件 1：还款事件时间戳 == 标签时刻 → max_1d_amount（inclusive）泄漏
    leak_row_a = labels[LABEL_SETS[0][0]][37]
    rows.append(
        {
            "entity_key": leak_row_a["entity_key"],
            "event_type": EventType.REPAY.value,
            "event_ts": leak_row_a["label_time"],
            "amount": 3860.00,
            "channel": "app",
            "source_ref": "EVT-BOUNDARY-1",
        }
    )
    # 边界事件 2：金额为 0 的申请事件时间戳 == 标签时刻 → cnt_30d_apply（inclusive）泄漏
    leak_row_b = labels[LABEL_SETS[0][0]][211]
    rows.append(
        {
            "entity_key": leak_row_b["entity_key"],
            "event_type": EventType.APPLY.value,
            "event_ts": leak_row_b["label_time"],
            "amount": 0.0,
            "channel": "h5",
            "source_ref": "EVT-BOUNDARY-2",
        }
    )
    rows.sort(key=lambda item: (item["event_ts"], item["entity_key"]))
    return rows


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #
def _ensure_sample_job(db: Session, code: str, name: str, label_sets, feature_ids, actor: str, run: bool):
    job = db.scalar(select(SampleJob).where(SampleJob.code == code))
    if job is not None:
        return job, False
    label_times = [
        value
        for value in db.scalars(select(LabelRow.label_time).where(LabelRow.label_set_id.in_(label_sets)))
    ]
    range_start = min(label_times) if label_times else None
    range_end = max(label_times) if label_times else None
    features = list(db.scalars(select(FeatureDef).where(FeatureDef.id.in_(feature_ids))))
    from src.services import feature_service

    specs = feature_service.to_specs(db, feature_ids)
    job = SampleJob(
        code=code,
        name=name,
        status="pending",
        label_set_ids=label_sets,
        feature_ids=feature_ids,
        range_start=range_start,
        range_end=range_end,
        boundary_policy=sample_service._effective_boundary(features),
        params_snapshot={
            "engine": "pit_engine",
            "engine_version": "1.0.0",
            "label_set_ids": label_sets,
            "label_set_codes": [s.code for s in db.scalars(select(LabelSet).where(LabelSet.id.in_(label_sets)))],
            "features": feature_service.snapshot_for_job(specs),
            "range_start": iso_z(range_start),
            "range_end": iso_z(range_end),
            "matrix_limit": sample_service.MATRIX_LIMIT,
            "created_at": iso_z(utcnow()),
        },
        created_by=actor,
        is_seed=True,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    if run:
        sample_service.execute_job(db, job)
    return job, True


def _ensure_failed_job(db: Session, label_sets, feature_ids, actor: str):
    code = "SJ-SEED-002"
    job = db.scalar(select(SampleJob).where(SampleJob.code == code))
    if job is not None:
        return job, False
    job = SampleJob(
        code=code,
        name="样本炼制 · 时间戳试验（预期失败）",
        status="pending",
        label_set_ids=label_sets,
        feature_ids=feature_ids,
        range_start=None,
        range_end=None,
        boundary_policy=BoundaryPolicy.STRICT.value,
        params_snapshot={
            "engine": "pit_engine",
            "engine_version": "1.0.0",
            "label_set_ids": label_sets,
            "range_start": "2026-02-23 24:00:00",
            "range_end": "2026-03-15 00:00:00",
            "features": [],
            "matrix_limit": sample_service.MATRIX_LIMIT,
            "created_at": iso_z(utcnow()),
        },
        created_by=actor,
        is_seed=True,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    sample_service.execute_job(db, job)
    return job, True


def _ensure_backfill(db: Session, anchor: datetime, feature_ids, actor: str) -> BackfillTask:
    """保留原有一致性回填（agg_engine full==incremental 证明）。"""
    from src.schemas.backfill import BackfillCreate

    code = "BF-SEED-001"
    task = db.scalar(select(BackfillTask).where(BackfillTask.code == code))
    if task is not None:
        return task
    payload = BackfillCreate(
        name="30 天回填 · full/incremental 一致性",
        mode="full",
        range_start=iso_z(anchor - timedelta(days=30)),
        range_end=iso_z(anchor),
        feature_ids=feature_ids,
    )

    class _Actor:
        email = actor

    task = backfill_service.create_backfill(db, payload, _Actor())
    task.code = code
    task.is_seed = True
    db.commit()
    db.refresh(task)
    backfill_service.execute_backfill(db, task)
    return task


def _publish_boundary_fix(db: Session, anchor: datetime, actor: str):
    """口径修正：cnt_30d_apply 从 inclusive 改回 strict，产出 v2 版本。

    ``effective_from`` 锚定在 40 天前（UTC 零点），保证演示样本（45 天内）
    的标签时间都能适用新版本——这样「新回填」会真实替换特征列。
    """
    from src.models import FeatureVersion
    from src.services import feature_service

    feature = db.scalar(select(FeatureDef).where(FeatureDef.code == "cnt_30d_apply"))
    effective_from = (anchor - timedelta(days=40)).replace(hour=0, minute=0, second=0, microsecond=0)
    existing_v2 = db.scalar(
        select(FeatureVersion).where(
            FeatureVersion.feature_id == feature.id, FeatureVersion.version_no == 2
        )
    )
    if existing_v2 is not None:
        return feature, effective_from
    feature.boundary_policy = BoundaryPolicy.STRICT.value
    feature.version = 2
    feature.updated_at = utcnow()
    db.add(
        FeatureVersion(
            feature_id=feature.id,
            version_no=2,
            snapshot=feature_service.snapshot(feature),
            change_note="口径修正：inclusive → strict，排除标签时刻事件（定向回填依据）",
            changed_by=actor,
            effective_from=effective_from,
        )
    )
    db.commit()
    db.refresh(feature)
    return feature, effective_from


def _create_seed_backfill(db: Session, code: str, name: str, anchor: datetime,
                          feature_ids: list[int], actor: str,
                          start_offset_days: int, end_offset_days: int,
                          execute: bool = True,
                          version_override: dict[str, int] | None = None) -> BackfillTask:
    from src.schemas.backfill import BackfillCreate

    existing = db.scalar(select(BackfillTask).where(BackfillTask.code == code))
    if existing is not None:
        return existing

    class _Actor:
        email = actor

    payload = BackfillCreate(
        name=name,
        mode="full",
        range_start=iso_z(anchor - timedelta(days=start_offset_days)),
        range_end=iso_z(anchor - timedelta(days=end_offset_days)),
        feature_ids=feature_ids,
    )
    task = backfill_service.create_backfill(db, payload, _Actor())
    task.code = code
    task.is_seed = True
    if version_override:
        # 模拟「旧口径任务」：执行前把锁定版本改回历史版本
        task.feature_versions = version_override
        db.commit()
        db.refresh(task)
    else:
        db.commit()
        db.refresh(task)
    if execute:
        backfill_service.execute_backfill(db, task)
    return task


def _ensure_versioned_backfills(db: Session, anchor: datetime, feature_ids: list[int],
                                target_feature: FeatureDef, actor: str) -> None:
    """版本化回填故事线（严格按顺序执行，模拟迟到到达）：

    1. BF-SEED-002 v2 新口径回填（覆盖近 20 天）→ 受影响样本特征列 replaced；
    2. BF-SEED-003 v1 旧口径回填迟到（同范围，执行前锁定回 v1）→ 已是 v2 的样本
       当前值保留并记 skipped_stale + is_conflict=true；
    3. 对 BF-SEED-002 再执行一次 → 整任务 skipped，不新增不覆盖。
    """
    # 1) 新口径回填（任务发起时锁定 v2）
    new_task = _create_seed_backfill(
        db,
        "BF-SEED-002",
        "定向回填 · cnt_30d_apply strict 口径修正（v2）",
        anchor,
        [target_feature.id],
        actor,
        start_offset_days=20,
        end_offset_days=0,
    )

    # 2) 旧口径回填迟到：执行前锁定 v1，模拟旧任务晚到
    _create_seed_backfill(
        db,
        "BF-SEED-003",
        "迟到回填 · cnt_30d_apply 旧 inclusive 口径（v1，应被跳过）",
        anchor,
        [target_feature.id],
        actor,
        start_offset_days=20,
        end_offset_days=0,
        version_override={str(target_feature.id): 1},
    )

    # 3) 同一新任务重复运行：幂等短路，不新增不覆盖
    backfill_service.execute_backfill(db, new_task)


def _counts(db: Session) -> dict[str, int]:
    seed_jobs = select(SampleJob.id).where(SampleJob.is_seed.is_(True)).subquery()
    return {
        "accounts": int(
            db.scalar(
                select(func.count(func.distinct(EntityEvent.entity_key))).where(
                    EntityEvent.is_seed.is_(True)
                )
            )
            or 0
        ),
        "events": int(
            db.scalar(select(func.count(EntityEvent.id)).where(EntityEvent.is_seed.is_(True))) or 0
        ),
        "features": int(
            db.scalar(select(func.count(FeatureDef.id)).where(FeatureDef.is_seed.is_(True))) or 0
        ),
        "label_sets": int(
            db.scalar(select(func.count(LabelSet.id)).where(LabelSet.is_seed.is_(True))) or 0
        ),
        "jobs": int(db.scalar(select(func.count(SampleJob.id)).where(SampleJob.is_seed.is_(True))) or 0),
        "samples": int(
            db.scalar(
                select(func.count(SampleRow.id)).where(SampleRow.job_id.in_(select(seed_jobs.c.id)))
            )
            or 0
        ),
        "backfills": int(
            db.scalar(select(func.count(BackfillTask.id)).where(BackfillTask.is_seed.is_(True))) or 0
        ),
        "backfill_results": int(
            db.scalar(select(func.count(BackfillResult.id))) or 0
        ),
        "users": int(db.scalar(select(func.count(User.id))) or 0),
    }


def run_seed() -> dict[str, int]:
    anchor = _anchor()
    db: Session = SessionLocal()
    try:
        owner = None
        for email, display_name, role, quota in USERS:
            user = _upsert_user(db, email, display_name, role, quota)
            if role == Role.OWNER:
                owner = user
        db.commit()

        features = [_ensure_feature(db, spec, owner, anchor) for spec in FEATURES]
        db.commit()
        feature_ids = [feature.id for feature in features]

        label_sets = [
            _ensure_label_set(db, code, name, description) for code, name, description in LABEL_SETS
        ]
        db.commit()

        labels = _generate_labels(anchor)
        for (code, _, _), label_set in zip(LABEL_SETS, label_sets):
            _ensure_label_rows(db, label_set, labels[code])
        db.commit()

        events = _generate_events(anchor, labels)
        _ensure_events(db, events)
        db.commit()

        completed_job, _ = _ensure_sample_job(
            db,
            "SJ-SEED-001",
            "样本炼制 · 2026Q1 双标签集（4 特征）",
            [label_set.id for label_set in label_sets],
            feature_ids,
            owner.email,
            run=True,
        )
        _ensure_failed_job(db, [label_sets[0].id], feature_ids, owner.email)
        _ensure_backfill(db, anchor, feature_ids, owner.email)

        # 版本化回填故事线：口径修正 v2 → 新回填替换 → 旧回填迟到跳过/冲突 → 重跑幂等
        fixed_feature, _ = _publish_boundary_fix(db, anchor, owner.email)
        _ensure_versioned_backfills(db, anchor, feature_ids, fixed_feature, owner.email)

        summary = _counts(db)
        line = " ".join(f"{key}={value}" for key, value in summary.items())
        print(f"SEED_SUMMARY {line}", flush=True)
        log_event("SEED_SUMMARY", **summary)
        return summary
    finally:
        db.close()


if __name__ == "__main__":
    run_seed()
